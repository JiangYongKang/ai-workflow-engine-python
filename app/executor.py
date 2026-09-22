"""DAG 调度执行器。

职责：
- 按依赖拓扑并行调度（并发度上限可配置）；
- 上游全部成功才放行下游；上游失败/取消/超时 -> 下游标记 SKIPPED（与 FAILED 可区分）；
- 步骤级超时、可重试/不可重试失败分类，指数退避且重试次数受配置上限约束；
- 幂等副作用：带 idempotency_key 的步骤，其效果账本中已有记录时直接命中缓存，
  不再调用动作，保证同一逻辑副作用只生效一次；
- 取消/运行超时：停止调度、回收未开始的任务、把未终态步骤归为 CANCELLED，
  不留半更新状态；迟到的工作线程结果会被丢弃。
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from app.actions import counted_call, get_action
from app.errors import FatalStepError, RetryableStepError, WorkflowError
from app.models import (
    RunRecord,
    RunStatus,
    StepRecord,
    StepStatus,
    TERMINAL_STEP_STATES,
)

_FAILURE_DEPS = frozenset(
    {StepStatus.FAILED, StepStatus.TIMED_OUT, StepStatus.CANCELLED, StepStatus.SKIPPED}
)
_MAX_BACKOFF_SECONDS = 5.0
_POLL_INTERVAL = 0.005


class _AttemptOutcome:
    def __init__(self, output: Any = None, error: WorkflowError | None = None) -> None:
        self.output = output
        self.error = error


class Executor:
    def __init__(self, max_concurrency: int = 4, run_timeout_seconds: float = 60.0,
                 backoff_base_seconds: float = 0.05, max_retries_cap: int = 5) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须 >= 1")
        self.max_concurrency = max_concurrency
        self.run_timeout_seconds = run_timeout_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self.max_retries_cap = max_retries_cap

    # ---- 单次尝试 -------------------------------------------------------

    def _attempt(self, record: RunRecord, step_id: str,
                 effects: dict[str, Any]) -> _AttemptOutcome:
        step_def = next(s for s in record.definition.steps if s.step_id == step_id)
        params = dict(step_def.params)
        params["_effect_ledger"] = effects
        key = step_def.idempotency_key
        if key is not None and key in effects:
            # 幂等命中：副作用已生效过，直接返回缓存结果，不再调用动作
            return _AttemptOutcome(output={"recorded": key,
                                           "count": effects[key],
                                           "idempotent_hit": True})
        try:
            action = get_action(step_def.action)
            output = counted_call(step_def.action, action, params)
            return _AttemptOutcome(output=output)
        except WorkflowError as exc:  # 已归一化的错误类型
            return _AttemptOutcome(error=exc)
        except Exception as exc:  # 底层异常归一化为不可重试失败，不原样透出
            return _AttemptOutcome(
                error=FatalStepError(f"动作执行异常（已归一化）: {type(exc).__name__}"))

    # ---- 主调度循环 -----------------------------------------------------

    def run(self, record: RunRecord, cancel_event: threading.Event) -> None:
        deadline = time.monotonic() + self.run_timeout_seconds
        effects: dict[str, Any] = {}
        deps = {s.step_id: set(s.depends_on) for s in record.definition.steps}
        step_defs = {s.step_id: s for s in record.definition.steps}
        attempts_left = {
            sid: min(step_defs[sid].max_retries, self.max_retries_cap) for sid in deps
        }
        backoff_until: dict[str, float] = {}
        in_flight: dict[str, Any] = {}
        timed_out = False

        pool = ThreadPoolExecutor(max_workers=self.max_concurrency,
                                  thread_name_prefix=f"run-{record.run_id[:8]}")
        try:
            with record.lock:
                record.status = RunStatus.RUNNING
            while True:
                now = time.monotonic()
                if cancel_event.is_set():
                    break
                if now >= deadline:
                    timed_out = True
                    break

                # 收集完成的尝试
                done_ids = [sid for sid, (fut, _deadline) in in_flight.items()
                            if fut.done()]
                for sid in done_ids:
                    fut, step_deadline = in_flight.pop(sid)
                    outcome: _AttemptOutcome = fut.result()
                    with record.lock:
                        rec = record.steps[sid]
                        if rec.status in TERMINAL_STEP_STATES:
                            continue  # 迟到结果，状态已终结，丢弃
                        if outcome.error is None:
                            rec.status = StepStatus.SUCCEEDED
                            rec.output = outcome.output
                            continue
                        err = outcome.error
                        rec.error = f"{err.code}: {err}"
                        retryable = isinstance(err, RetryableStepError)
                        if retryable and attempts_left[sid] > 0:
                            attempts_left[sid] -= 1
                            backoff = min(
                                self.backoff_base_seconds
                                * 2 ** (rec.attempts - 1),
                                _MAX_BACKOFF_SECONDS)
                            backoff_until[sid] = time.monotonic() + backoff
                            rec.status = StepStatus.PENDING  # 等待重试
                        elif isinstance(err, RetryableStepError):
                            rec.status = StepStatus.FAILED  # 重试耗尽
                        elif isinstance(err, FatalStepError):
                            rec.status = StepStatus.FAILED
                        else:
                            rec.status = StepStatus.FAILED

                # 检查在飞步骤的步骤级超时
                for sid, (fut, step_deadline) in list(in_flight.items()):
                    if step_deadline is not None and now >= step_deadline:
                        in_flight.pop(sid)
                        fut.cancel()  # 尽力取消；线程结果将被丢弃
                        with record.lock:
                            rec = record.steps[sid]
                            if rec.status in TERMINAL_STEP_STATES:
                                continue
                            rec.error = "step_timeout: 步骤执行超时"
                            if attempts_left[sid] > 0:
                                attempts_left[sid] -= 1
                                backoff_until[sid] = now + self.backoff_base_seconds
                                rec.status = StepStatus.PENDING
                            else:
                                rec.status = StepStatus.TIMED_OUT

                # 推进状态机：放行就绪步骤 / 传播 SKIPPED
                for sid, rec in list(record.steps.items()):
                    with record.lock:
                        status = rec.status
                    if status is not StepStatus.PENDING or sid in in_flight:
                        continue
                    if backoff_until.get(sid, 0.0) > now:
                        continue
                    dep_states = {record.steps[d].status for d in deps[sid]}
                    if dep_states & _FAILURE_DEPS:
                        with record.lock:
                            record.steps[sid].status = StepStatus.SKIPPED
                            record.steps[sid].error = "上游未成功，步骤被跳过"
                        continue
                    if dep_states and dep_states != {StepStatus.SUCCEEDED}:
                        continue  # 上游仍在推进
                    step_def = step_defs[sid]
                    with record.lock:
                        rec = record.steps[sid]
                        rec.status = StepStatus.RUNNING
                        rec.attempts += 1
                    timeout = step_def.timeout_seconds
                    step_deadline = (now + timeout) if timeout is not None else None
                    fut = pool.submit(self._attempt, record, sid, effects)
                    in_flight[sid] = (fut, step_deadline)

                # 全部终态 -> 结束
                with record.lock:
                    all_terminal = all(
                        r.status in TERMINAL_STEP_STATES
                        for r in record.steps.values())
                if all_terminal and not in_flight:
                    break
                time.sleep(_POLL_INTERVAL)
        finally:
            # 取消/超时/正常结束：回收未开始的任务，等待在飞任务退出，不留后台推进
            pool.shutdown(wait=True, cancel_futures=True)

        with record.lock:
            if cancel_event.is_set() and not timed_out:
                for rec in record.steps.values():
                    if rec.status not in TERMINAL_STEP_STATES:
                        rec.status = StepStatus.CANCELLED
                        rec.error = "运行被取消"
                record.status = RunStatus.CANCELLED
                record.reason = "运行被调用者取消"
            elif timed_out:
                for rec in record.steps.values():
                    if rec.status not in TERMINAL_STEP_STATES:
                        rec.status = StepStatus.CANCELLED
                        rec.error = "运行超时，步骤被终止"
                record.status = RunStatus.TIMED_OUT
                record.reason = (f"运行超时：总耗时超过上限 "
                                 f"{self.run_timeout_seconds}s，已按策略终止")
            else:
                failed = [s.step_id for s in record.steps.values()
                          if s.status in (StepStatus.FAILED, StepStatus.TIMED_OUT)]
                if failed:
                    record.status = RunStatus.FAILED
                    record.reason = f"步骤失败: {sorted(failed)}"
                else:
                    record.status = RunStatus.SUCCEEDED
