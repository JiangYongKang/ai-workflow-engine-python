"""运行记录存储：幂等提交、所有者访问控制、线程安全状态查询。"""
from __future__ import annotations

import threading
import uuid

from .errors import AccessError, ErrorCode
from .models import RunRecord, StepRecord, WorkflowDefinition


class RunStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs: dict[str, RunRecord] = {}
        self._idempotency: dict[tuple[str, str], str] = {}  # (owner, key) -> run_id

    def submit(self, owner: str, idempotency_key: str,
               workflow: WorkflowDefinition) -> tuple[RunRecord, bool]:
        """幂等提交：返回 (记录, 是否新建)。同一 (owner, key) 重复提交返回首次记录，不产生第二次执行。"""
        with self._lock:
            existing = self._idempotency.get((owner, idempotency_key))
            if existing is not None:
                return self._runs[existing], False
            run = RunRecord(
                run_id=uuid.uuid4().hex,
                owner=owner,
                workflow=workflow,
                steps={s.step_id: StepRecord(step_id=s.step_id) for s in workflow.steps},
            )
            self._runs[run.run_id] = run
            self._idempotency[(owner, idempotency_key)] = run.run_id
            return run, True

    def get_for_owner(self, run_id: str, owner: str) -> RunRecord:
        """越权与不存在给出可区分的错误。"""
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise AccessError(ErrorCode.RUN_NOT_FOUND, f"run {run_id!r} does not exist")
            if run.owner != owner:
                raise AccessError(ErrorCode.FORBIDDEN, f"run {run_id!r} belongs to another owner")
            return run

    def get_internal(self, run_id: str) -> RunRecord:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise AccessError(ErrorCode.RUN_NOT_FOUND, f"run {run_id!r} does not exist")
            return run
