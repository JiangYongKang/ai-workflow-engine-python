"""领域模型：工作流定义、运行与步骤状态。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # 自身执行失败（含重试耗尽）
    SKIPPED = "SKIPPED"        # 上游失败/取消，未执行，与 FAILED 可区分
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
)
TERMINAL_STEP_STATUSES = frozenset(
    {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED, StepStatus.CANCELLED, StepStatus.TIMED_OUT}
)


@dataclass(frozen=True)
class StepDefinition:
    step_id: str
    action: str                     # 本地注册的处理器名（模拟模型/工具调用）
    depends_on: tuple[str, ...] = ()
    params: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float | None = None
    max_retries: int | None = None


@dataclass(frozen=True)
class WorkflowDefinition:
    workflow_id: str
    steps: tuple[StepDefinition, ...]


@dataclass
class StepRecord:
    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    error_code: str | None = None
    error_message: str | None = None
    output: Any = None
    started_at: float | None = None
    finished_at: float | None = None


@dataclass
class RunRecord:
    run_id: str
    owner: str
    workflow: WorkflowDefinition
    status: RunStatus = RunStatus.PENDING
    error_code: str | None = None
    error_message: str | None = None
    steps: dict[str, StepRecord] = field(default_factory=dict)
    created_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
