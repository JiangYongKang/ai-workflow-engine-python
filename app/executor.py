"""按依赖关系调度执行：并行、重试退避、超时、取消、运行级时限。"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .config import EngineConfig
from .errors import ErrorCode, WorkflowError
from .models import (
    RunRecord,
    RunStatus,
    StepDefinition,
    StepRecord,
    StepStatus,
    TERMINAL_STEP_STATUSES,
)
from .providers import EffectLedger, HandlerRegistry, StepContext
from .store import RunStore

_TIMEOUT_GRACE_SECONDS = 1.0  # 步骤超时后等待其协作退出的宽限


class _RunControl:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.cancel_event = threading.Event()
        self.done_event = threading.Event()
        self.ledger = EffectLedger()
        self.inflight: list[threading.Thread] = []


class RunExecutor:
    def __init__(self, config: EngineConfig, registry: HandlerRegistry, store: RunStore) -> None:
        self._config = config
        self._registry = registry
        self._store = store
        self._controls: dict[str, _RunControl] = {}
        self._controls_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=config.max_concurrency,
                                        thread_name_prefix="wf-step")
        self._coordinators = ThreadPoolExecutor(max_workers=8, thread_name_prefix="wf-coord")

    # ---- 对外接口 ----

    def start(self, run_id: str) -> None:
        """异步启动一次运行（后台协调线程），立即返回。"""
        ctl = _RunControl()
        with self._controls_lock:
            self._controls[run_id] = ctl
        self._coordinators.submit(self._coordinate, run_id, ctl)

    def cancel(self, run_id: str) -> None:
        """请求取消：协调循环停止调度新步骤，在途步骤收到取消信号后被等待退出。"""
        ctl = self._get_control(run_id)
        ctl.cancel_event.set()

    def wait(self, run_id: str, timeout: float | None = None) -> RunRecord:
        """等待运行到达终态（供测试与同步调用使用）。"""
        ctl = self._get_control(run_id)
        ctl.done_event.wait(timeout)
        return self._store.get_internal(run_id)

    def shutdown(self) -> None:
        with self._controls_lock:
            for ctl in self._controls.values():
                ctl.cancel_event.set()
        self._coordinators.shutdown(wait=True)
        self._pool.shutdown(wait=True)

    # ---- 内部实现 ----

    def _get_control(self, run_id: str) -> _RunControl:
        with self._controls_lock:
            ctl = self._controls.get(run_id)
        if ctl is None:
            raise WorkflowError(ErrorCode.RUN_NOT_FOUND, f"run {run_id!r} is not active")
        return ctl

    def _coordinate(self, run_id: str, ctl: _RunControl) -> None:
        """调度主循环：只启动全部上游已成功的步骤；上游失败则下游标记 SKIPPED。"""
        run = self._store.get_internal(run_id)
        steps = {s.step_id: s for s in run.workflow.steps}
        deadline = run.created_at + self._config.max_run_seconds
        futures: dict[str, object] = {}
        try:
            with ctl.lock:
                run.status = RunStatus.RUNNING
            while True:
                with ctl.lock:
                    # 运行级终止条件
                    if ctl.cancel_event.is_set() and run.status == RunStatus.RUNNING:
                        run.status = RunStatus.CANCELLED
                        run.error_code = ErrorCode.RUN_CANCELLED.value
                        run.error_message = "run was cancelled"
                    if run.status == RunStatus.RUNNING and time.monotonic() > deadline:
                        run.status = RunStatus.TIMED_OUT
                        run.error_code = ErrorCode.RUN_TIMEOUT.value
                        run.error_message = f"run exceeded {self._config.max_run_seconds}s limit"
                        ctl.cancel_event.set()
                    terminating = run.status in (RunStatus.CANCELLED, RunStatus.TIMED_OUT)

                    # 上游失败/终止 -> 下游 SKIPPED（与 FAILED 可区分，不静默丢弃）
                    for s in steps.values():
                        rec = run.steps[s.step_id]
                        if rec.status != StepStatus.PENDING:
                            continue
                        dep_recs = [run.steps[d] for d in s.depends_on]
                        if any(d.status in (StepStatus.FAILED, StepStatus.TIMED_OUT,
                                            StepStatus.SKIPPED, StepStatus.CANCELLED)
                               for d in dep_recs):
                            rec.status = StepStatus.SKIPPED
                            rec.error_code = ErrorCode.UPSTREAM_FAILED.value
                            rec.error_message = "one or more upstream steps did not succeed"
                            rec.finished_at = time.monotonic()

                    # 启动可运行步骤（受并发上限约束，由线程池排队实现）
                    if not terminating:
                        for s in steps.values():
                            rec = run.steps[s.step_id]
                            if rec.status != StepStatus.PENDING or s.step_id in futures:
                                continue
                            if all(run.steps[d].status == StepStatus.SUCCEEDED for d in s.depends_on):
                                rec.status = StepStatus.RUNNING
                                rec.started_at = time.monotonic()
                                fut = self._pool.submit(self._run_step, run, ctl, s)
                                futures[s.step_id] = fut

                    pending_any = any(r.status == StepStatus.PENDING for r in run.steps.values())
                    inflight_done = all(f.done() for f in futures.values())

                # 退出条件：正常路径下无 PENDING；终止路径下不再调度新步骤。
                # 两种情况都要求全部在途任务结束后才退出，保证无后台残留。
                if (terminating or not pending_any) and inflight_done:
                    break
                time.sleep(0.005)

            # 等待全部在途任务退出，保证无后台残留
            for f in futures.values():
                f.result()
            with ctl.lock:
                for rec in run.steps.values():
                    if rec.status == StepStatus.PENDING:
                        rec.status = StepStatus.CANCELLED
                        rec.error_code = run.error_code
                        rec.error_message = "step never started before run terminated"
                        rec.finished_at = time.monotonic()
                if run.status == RunStatus.RUNNING:
                    if all(r.status == StepStatus.SUCCEEDED for r in run.steps.values()):
                        run.status = RunStatus.SUCCEEDED
                    else:
                        run.status = RunStatus.FAILED
                        failed = [r.step_id for r in run.steps.values()
                                  if r.status in (StepStatus.FAILED, StepStatus.TIMED_OUT)]
                        run.error_code = ErrorCode.NON_RETRYABLE_FAILURE.value
                        run.error_message = f"steps failed: {failed}"
                run.finished_at = time.monotonic()
        finally:
            ctl.done_event.set()

    def _run_step(self, run: RunRecord, ctl: _RunControl, step: StepDefinition) -> None:
        """执行单步：重试退避受配置上限约束；超时与失败类型可区分。"""
        rec = run.steps[step.step_id]
        max_retries = min(step.max_retries if step.max_retries is not None
                          else self._config.max_retries, self._config.max_retries)
        timeout = step.timeout_seconds or self._config.default_step_timeout
        attempt = 0
        while True:
            attempt += 1
            with ctl.lock:
                rec.attempts = attempt
            err = self._attempt_once(run, ctl, step, rec, attempt, timeout)
            if err is None:
                return  # 成功
            if ctl.cancel_event.is_set():
                with ctl.lock:
                    rec.status = StepStatus.CANCELLED
                    rec.error_code = ErrorCode.RUN_CANCELLED.value
                    rec.error_message = "cancelled"
                    rec.finished_at = time.monotonic()
                return
            if err.code == ErrorCode.STEP_TIMEOUT:
                with ctl.lock:
                    rec.status = StepStatus.TIMED_OUT
                    rec.error_code = err.code.value
                    rec.error_message = err.message
                    rec.finished_at = time.monotonic()
                return
            if err.code == ErrorCode.RETRYABLE_FAILURE and attempt <= max_retries:
                backoff = self._config.retry_backoff_base * (2 ** (attempt - 1))
                ctl.cancel_event.wait(backoff)  # 可被取消打断的退避
                continue
            with ctl.lock:
                rec.status = StepStatus.FAILED
                rec.error_code = (ErrorCode.RETRIES_EXHAUSTED.value
                                  if err.code == ErrorCode.RETRYABLE_FAILURE else err.code.value)
                rec.error_message = err.message
                rec.finished_at = time.monotonic()
            return

    def _attempt_once(self, run: RunRecord, ctl: _RunControl, step: StepDefinition,
                      rec: StepRecord, attempt: int, timeout: float) -> WorkflowError | None:
        """单次尝试：在独立线程中执行以便施加超时；返回 None 表示成功。"""
        result: dict[str, object] = {}

        attempt_cancel = threading.Event()

        def target() -> None:
            ctx = StepContext(run.run_id, step.step_id, attempt, ctl.cancel_event,
                              ctl.ledger, attempt_cancel)
            try:
                result["output"] = self._registry.invoke(step.action, step.params, ctx)
            except WorkflowError as e:
                result["error"] = e

        t = threading.Thread(target=target, daemon=True,
                             name=f"wf-attempt-{run.run_id[:8]}-{step.step_id}")
        t.start()
        t.join(timeout)
        if t.is_alive():
            attempt_cancel.set()          # 通知处理器本次尝试已超时，协作退出
            t.join(_TIMEOUT_GRACE_SECONDS)
            return WorkflowError(ErrorCode.STEP_TIMEOUT,
                                 f"step {step.step_id!r} exceeded {timeout}s timeout")
        err = result.get("error")
        if err is not None:
            return err  # type: ignore[return-value]
        with ctl.lock:
            rec.status = StepStatus.SUCCEEDED
            rec.output = result.get("output")
            rec.finished_at = time.monotonic()
        return None
