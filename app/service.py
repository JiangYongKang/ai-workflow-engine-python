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
    ResumeLimitError,
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
        # 续跑幂等：(caller, run_id, idempotency_key) -> resume_id
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

    # ---- 失败续跑 --------------------------------------------------------

    def resume(self, caller: str, run_id: str,
               idempotency_key: str | None = None) -> tuple[str, RunRecord]:
        """对已失败/超时的运行发起续跑，返回 (resume_id, record)。

        - 仅运行的原提交者可以续跑，越权与不存在分别抛 RunForbidden / RunNotFound；
        - 仅 FAILED / TIMED_OUT 终态可续跑；运行中、成功、取消等一律 RunConflict；
        - 相同 (caller, run_id, idempotency_key) 重复请求只生效一次，
          返回同一个 resume_id；
        - 并发续跑（无幂等键或键不同）只允许一个成功，其余 RunConflict；
        - 单运行续跑次数受 max_resumes_per_run 约束，超限抛 ResumeLimitError。
        """
        record = self.get_run(caller, run_id)  # 复用「不存在/越权」校验

        if idempotency_key is not None:
            with self._lock:
                existing = self._resume_idempotency.get(
                    (caller, run_id, idempotency_key))
            if existing is not None:
                return existing, record  # 幂等命中，返回一致的结果标识

        resume_id = uuid.uuid4().hex
        new_event = threading.Event()
        thread = threading.Thread(
            target=self._executor.run, args=(record, new_event),
            name=f"resume-{run_id[:8]}", daemon=True)

        with self._lock:
            if idempotency_key is not None:
                # 锁内二次检查，挡住同键并发
                existing = self._resume_idempotency.get(
                    (caller, run_id, idempotency_key))
                if existing is not None:
                    return existing, record

            with record.lock:
                if record.status not in (RunStatus.FAILED, RunStatus.TIMED_OUT):
                    raise RunConflictError(
                        f"运行当前状态 {record.status.value}，"
                        f"仅失败/超时的运行可以续跑")
                if record.resume_count >= self._max_resumes_per_run:
                    raise ResumeLimitError(
                        f"续跑次数 {record.resume_count} 已达单运行上限 "
                        f"{self._max_resumes_per_run}，拒绝再次续跑")
                # 先重置步骤状态，再翻运行状态：临界区内不存在半更新的中间态
                self._executor.reset_for_resume(record)
                record.resume_count += 1
                record.reason = f"第 {record.resume_count} 次续跑中"
                record.status = RunStatus.RUNNING

            if idempotency_key is not None:
                self._resume_idempotency[(caller, run_id, idempotency_key)] = resume_id
            # 替换上一轮的 cancel event / thread；旧线程早在终态时已退出
            self._cancel_events[run_id] = new_event
            self._threads[run_id] = thread

        thread.start()
        return resume_id, record

    # ---- 测试辅助 -------------------------------------------------------

    def wait(self, run_id: str, timeout: float = 10.0) -> RunRecord:
        """阻塞等待运行结束（供测试与本地验证使用）。"""
        with self._lock:
            thread = self._threads[run_id]
        thread.join(timeout=timeout)
        with self._lock:
            return self._runs[run_id]
