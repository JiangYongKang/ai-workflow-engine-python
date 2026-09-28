"""工作流服务：提交（幂等）、查询、取消、归属校验、资源上限。

线程模型：submit 校验通过后为每次运行启动一个守护线程执行 Executor.run，
运行记录的所有读写都经 RunRecord.lock / 服务级锁保护，
保证并发查询与并发推进下状态一致。
"""
from __future__ import annotations

import threading
import uuid
from typing import Any

from app.errors import (
    ResourceLimitError,
    ResumeConflictError,
    ResumeLimitExceededError,
    ResumeNotAllowedError,
    RunConflictError,
    RunForbiddenError,
    RunNotFoundError,
)
from app.executor import Executor
from app.models import (
    RunRecord,
    RunStatus,
    StepRecord,
    TERMINAL_RUN_STATES,
)
from app.validation import parse_definition, validate_definition

# 允许发起续跑的终态：失败或超时；成功/取消不可续跑
_RESUMABLE_STATES = frozenset({RunStatus.FAILED, RunStatus.TIMED_OUT})


class WorkflowService:
    def __init__(self, executor: Executor | None = None,
                 max_steps_per_run: int = 64,
                 max_resumes_per_run: int = 3) -> None:
        self._executor = executor or Executor()
        self._max_steps_per_run = max_steps_per_run
        self._max_resumes_per_run = max_resumes_per_run
        self._lock = threading.Lock()
        self._runs: dict[str, RunRecord] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        # (run_id, owner, resume_idempotency_key) -> resume_id
        self._resume_idempotency: dict[tuple[str, str, str], str] = {}

    # ---- 提交 -----------------------------------------------------------

    def submit(self, owner: str, raw_definition: dict[str, Any],
               idempotency_key: str | None = None) -> str:
        """校验并启动一次运行，返回 run_id。

        - 定义不合法：抛错，不产生任何运行记录；
        - 步骤数超限：抛 ResourceLimitError；
        - 相同 (owner, idempotency_key) 重复提交：返回首次的 run_id，不重复执行。
        """
        if idempotency_key is not None:
            with self._lock:
                existing = self._idempotency.get((owner, idempotency_key))
            if existing is not None:
                return existing

        # 解析与静态校验发生在创建任何记录之前
        definition = parse_definition(raw_definition)
        validate_definition(definition)
        if len(definition.steps) > self._max_steps_per_run:
            raise ResourceLimitError(
                f"步骤数 {len(definition.steps)} 超过单次运行上限 "
                f"{self._max_steps_per_run}")

        run_id = uuid.uuid4().hex
        record = RunRecord(
            run_id=run_id,
            owner=owner,
            definition=definition,
            steps={s.step_id: StepRecord(s.step_id) for s in definition.steps},
        )
        cancel_event = threading.Event()
        thread = threading.Thread(
            target=self._executor.run, args=(record, cancel_event),
            name=f"workflow-{run_id[:8]}", daemon=True)
        with self._lock:
            if idempotency_key is not None:
                existing = self._idempotency.get((owner, idempotency_key))
                if existing is not None:
                    return existing
                self._idempotency[(owner, idempotency_key)] = run_id
            self._runs[run_id] = record
            self._cancel_events[run_id] = cancel_event
            self._threads[run_id] = thread
        thread.start()
        return run_id

    # ---- 查询 -----------------------------------------------------------

    def get_run(self, caller: str, run_id: str) -> RunRecord:
        """查询运行；越权与不存在给出可区分的错误。"""
        with self._lock:
            record = self._runs.get(run_id)
        if record is None:
            raise RunNotFoundError(f"运行不存在: {run_id}")
        if record.owner != caller:
            raise RunForbiddenError(f"调用者 {caller!r} 无权访问运行 {run_id}")
        return record

    # ---- 取消 -----------------------------------------------------------

    def cancel(self, caller: str, run_id: str) -> RunRecord:
        """取消运行并等待执行线程回收，返回后不留后台推进的任务。"""
        record = self.get_run(caller, run_id)  # 复用归属校验
        with self._lock:
            event = self._cancel_events[run_id]
            thread = self._threads[run_id]
        with record.lock:
            # 终态检查与取消置位在同一临界区，保证并发取消只有一次生效
            if record.status in TERMINAL_RUN_STATES or event.is_set():
                raise RunConflictError(
                    f"运行已处于终态或取消中（{record.status.value}），无法取消")
            event.set()
        thread.join()
        return record

    # ---- 续跑 -----------------------------------------------------------

    def resume(self, caller: str, run_id: str,
               idempotency_key: str | None = None) -> str:
        """对失败/超时终态的运行发起续跑，返回 resume_id。

        - 运行不存在 / 非提交者：404 / 403（复用归属校验）；
        - 初始执行尚未结束：ResumeNotAllowedError；
        - 终态不是失败或超时（成功/取消）：ResumeNotAllowedError；
        - 续跑次数超过上限：ResumeLimitExceededError；
        - 已有续跑在推进（并发续跑）：只有一个成功，其余 ResumeConflictError；
        - 相同 (run_id, caller, idempotency_key) 重复提交：返回首次的
          resume_id，不产生第二次续跑。
        """
        record = self.get_run(caller, run_id)  # 404 / 403
        with self._lock:
            if idempotency_key is not None:
                existing = self._resume_idempotency.get(
                    (run_id, caller, idempotency_key))
                if existing is not None:
                    return existing
            with record.lock:
                status = record.status
                if status not in TERMINAL_RUN_STATES:
                    # 运行仍在推进：续跑进行中 -> 并发冲突；初始执行中 -> 明确拒绝
                    if record.resume_count > 0:
                        raise ResumeConflictError(
                            "该运行已有续跑在推进中，并发续跑只允许一次生效")
                    raise ResumeNotAllowedError(
                        f"运行尚未结束（{status.value}），不能续跑")
                if status not in _RESUMABLE_STATES:
                    raise ResumeNotAllowedError(
                        f"运行终态为 {status.value}，仅失败/超时可以续跑")
                if record.resume_count >= self._max_resumes_per_run:
                    raise ResumeLimitExceededError(
                        f"续跑次数已达配置上限 {self._max_resumes_per_run} 次，"
                        f"拒绝再次续跑")
                # 终态 -> RUNNING 的占位与计数在同一临界区，保证并发续跑只有一次生效
                record.status = RunStatus.RUNNING
                record.resume_count += 1
                resume_id = uuid.uuid4().hex
                if idempotency_key is not None:
                    self._resume_idempotency[
                        (run_id, caller, idempotency_key)] = resume_id
                cancel_event = threading.Event()
                thread = threading.Thread(
                    target=self._executor.run,
                    kwargs={"record": record, "cancel_event": cancel_event,
                            "resume": True},
                    name=f"resume-{run_id[:8]}-{record.resume_count}",
                    daemon=True)
                self._cancel_events[run_id] = cancel_event
                self._threads[run_id] = thread
        thread.start()
        return resume_id

    # ---- 测试辅助 -------------------------------------------------------

    def wait(self, run_id: str, timeout: float = 10.0) -> RunRecord:
        """阻塞等待运行结束（供测试与本地验证使用）。"""
        with self._lock:
            thread = self._threads[run_id]
        thread.join(timeout=timeout)
        with self._lock:
            return self._runs[run_id]
