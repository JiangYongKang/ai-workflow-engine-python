"""FastAPI 对外接口。

- 调用者身份通过 X-Owner 请求头传入（本地验证用，无真实凭据）；
- 所有内部错误归一化为 {error: {code, message}}，底层异常不原样透出；
- 不同错误类别映射到可区分的 HTTP 状态码。
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, Response
from pydantic import BaseModel

from app.errors import (
    CyclicDependencyError,
    DefinitionError,
    DuplicateStepIdError,
    MissingDependencyError,
    ResourceLimitError,
    RunConflictError,
    RunForbiddenError,
    RunNotFoundError,
    WorkflowError,
)
from app.models import RunRecord
from app.service import WorkflowService

app = FastAPI(title="AI Workflow Engine")
service = WorkflowService()

_STATUS_BY_CODE = {
    "invalid_definition": 422,
    "cyclic_dependency": 422,
    "missing_dependency": 422,
    "duplicate_step_id": 422,
    "resource_limit_exceeded": 413,
    "run_not_found": 404,
    "run_forbidden": 403,
    "run_conflict": 409,
}


@app.exception_handler(WorkflowError)
async def workflow_error_handler(_request, exc: WorkflowError) -> Response:
    from fastapi.responses import JSONResponse
    status = _STATUS_BY_CODE.get(exc.code, 500)
    return JSONResponse(status_code=status,
                        content={"error": {"code": exc.code, "message": str(exc)}})


class SubmitRequest(BaseModel):
    definition: dict[str, Any]
    idempotency_key: str | None = None


def _serialize(record: RunRecord) -> dict[str, Any]:
    with record.lock:
        return {
            "run_id": record.run_id,
            "owner": record.owner,
            "status": record.status.value,
            "reason": record.reason,
            "steps": {
                sid: {
                    "status": r.status.value,
                    "attempts": r.attempts,
                    "output": r.output,
                    "error": r.error,
                } for sid, r in record.steps.items()
            },
        }


@app.post("/runs", status_code=201)
def submit_run(req: SubmitRequest,
               x_owner: str = Header(default="anonymous")) -> dict[str, Any]:
    run_id = service.submit(x_owner, req.definition, req.idempotency_key)
    return {"run_id": run_id}


@app.get("/runs/{run_id}")
def get_run(run_id: str, x_owner: str = Header(default="anonymous")) -> dict[str, Any]:
    return _serialize(service.get_run(x_owner, run_id))


@app.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, x_owner: str = Header(default="anonymous")) -> dict[str, Any]:
    return _serialize(service.cancel(x_owner, run_id))


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
