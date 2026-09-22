"""FastAPI 接口层：提交/查询/取消工作流运行。"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .config import EngineConfig
from .errors import ErrorCode, WorkflowError
from .executor import RunExecutor
from .models import RunRecord, RunStatus, StepRecord, TERMINAL_RUN_STATUSES
from .providers import default_registry
from .store import RunStore
from .validation import parse_definition, validate_definition

_ERROR_HTTP_STATUS = {
    ErrorCode.INVALID_DEFINITION: 400,
    ErrorCode.DUPLICATE_STEP_ID: 400,
    ErrorCode.MISSING_DEPENDENCY: 400,
    ErrorCode.CYCLE_DETECTED: 400,
    ErrorCode.STEP_LIMIT_EXCEEDED: 400,
    ErrorCode.RUN_NOT_FOUND: 404,
    ErrorCode.FORBIDDEN: 403,
}


def _error_response(exc: WorkflowError) -> JSONResponse:
    status = _ERROR_HTTP_STATUS.get(exc.code, 500)
    return JSONResponse(status_code=status,
                        content={"error": {"code": exc.code.value, "message": exc.message}})


def _step_view(rec: StepRecord) -> dict[str, Any]:
    return {
        "step_id": rec.step_id,
        "status": rec.status.value,
        "attempts": rec.attempts,
        "error_code": rec.error_code,
        "error_message": rec.error_message,
        "output": rec.output,
    }


def _run_view(run: RunRecord) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "owner": run.owner,
        "workflow_id": run.workflow.workflow_id,
        "status": run.status.value,
        "error_code": run.error_code,
        "error_message": run.error_message,
        "terminal": run.status in TERMINAL_RUN_STATUSES,
        "steps": {sid: _step_view(rec) for sid, rec in run.steps.items()},
    }


class SubmitRequest(BaseModel):
    workflow: dict[str, Any]
    idempotency_key: str
    wait: bool = False  # 同步等待运行结束（便于本地验证）


def create_app(config: EngineConfig | None = None) -> FastAPI:
    config = config or EngineConfig()
    store = RunStore()
    executor = RunExecutor(config, default_registry(), store)

    app = FastAPI(title="AI Workflow Engine")
    app.state.config = config
    app.state.store = store
    app.state.executor = executor

    @app.exception_handler(WorkflowError)
    async def workflow_error_handler(_request: Any, exc: WorkflowError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(Exception)
    async def unknown_error_handler(_request: Any, exc: Exception) -> JSONResponse:
        # 底层异常不原样透出
        return _error_response(WorkflowError(ErrorCode.INTERNAL, "internal error"))

    @app.post("/runs", status_code=201)
    def submit_run(req: SubmitRequest, x_owner: str = Header(default="")) -> Any:
        if not x_owner:
            raise HTTPException(status_code=401, detail="missing X-Owner header")
        if not req.idempotency_key:
            raise WorkflowError(ErrorCode.INVALID_DEFINITION, "idempotency_key is required")
        defn = parse_definition(req.workflow)          # 校验失败直接拒绝，不留运行记录
        validate_definition(defn, config.max_steps_per_run)
        run, created = store.submit(x_owner, req.idempotency_key, defn)
        if created:
            executor.start(run.run_id)
        if req.wait and created:
            run = executor.wait(run.run_id, timeout=config.max_run_seconds + 5)
        return _run_view(run)

    @app.get("/runs/{run_id}")
    def get_run(run_id: str, x_owner: str = Header(default="")) -> Any:
        if not x_owner:
            raise HTTPException(status_code=401, detail="missing X-Owner header")
        return _run_view(store.get_for_owner(run_id, x_owner))

    @app.post("/runs/{run_id}/cancel")
    def cancel_run(run_id: str, x_owner: str = Header(default="")) -> Any:
        if not x_owner:
            raise HTTPException(status_code=401, detail="missing X-Owner header")
        run = store.get_for_owner(run_id, x_owner)
        if run.status not in TERMINAL_RUN_STATUSES:
            executor.cancel(run_id)
        return _run_view(run)

    @app.on_event("shutdown")
    def _shutdown() -> None:
        executor.shutdown()

    return app
