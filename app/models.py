"""核心数据模型与状态枚举（骨架：仅类型定义）。"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field
from typing import Any


class StepStatus(str, enum.Enum):
    PENDING = "PENDING"        # 等待上游完成
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # 不可重试失败或重试耗尽
    SKIPPED = "SKIPPED"        # 上游失败，未执行（与 FAILED 可区分）
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL_STEP_STATES = frozenset(
    {StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.SKIPPED,
     StepStatus.CANCELLED, StepStatus.TIMED_OUT}
)
TERMINAL_RUN_STATES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.TIMED_OUT}
)


@dataclass(frozen=True)
class StepDefinition:
    step_id: str
    action: str                       # 本地动作名，如 "echo" / "fail" / "sleep"
    params: dict[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    timeout_seconds: float | None = None
    max_retries: int = 0
    idempotency_key: str | None = None


@dataclass(frozen=True)
class WorkflowDefinition:
    workflow_id: str
    steps: tuple[StepDefinition, ...]


@dataclass
class StepRecord:
    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    output: Any = None
    error: str | None = None


@dataclass
class RunRecord:
    run_id: str
    owner: str
    definition: WorkflowDefinition
    status: RunStatus = RunStatus.PENDING
    steps: dict[str, StepRecord] = field(default_factory=dict)
    reason: str | None = None
    # 持久化的副作用账本：跨轮次保留，续跑时带 idempotency_key 的步骤命中后
    # 不再真正执行，保证同一逻辑副作用只生效一次
    effects: dict[str, Any] = field(default_factory=dict)
    # 幂等步骤首次执行的真实输出缓存：key -> output，跨轮次复用
    effect_outputs: dict[str, Any] = field(default_factory=dict)
    # 已成功发起过的续跑次数（0 表示尚未续跑）
    resume_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
