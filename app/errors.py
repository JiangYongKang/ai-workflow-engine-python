"""错误分类：所有对外错误都归一化为可区分的类型，底层异常不原样透出。"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    # 定义校验类（提交即拒绝）
    INVALID_DEFINITION = "INVALID_DEFINITION"
    DUPLICATE_STEP_ID = "DUPLICATE_STEP_ID"
    MISSING_DEPENDENCY = "MISSING_DEPENDENCY"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    # 资源限制类
    STEP_LIMIT_EXCEEDED = "STEP_LIMIT_EXCEEDED"
    RUN_TIMEOUT = "RUN_TIMEOUT"
    # 步骤执行类
    STEP_TIMEOUT = "STEP_TIMEOUT"
    RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
    NON_RETRYABLE_FAILURE = "NON_RETRYABLE_FAILURE"
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    # 访问控制类
    RUN_NOT_FOUND = "RUN_NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"
    # 运行状态类
    RUN_CANCELLED = "RUN_CANCELLED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    INTERNAL = "INTERNAL"


class WorkflowError(Exception):
    """所有对外暴露错误的基类，携带可区分的错误码。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class DefinitionError(WorkflowError):
    """工作流定义不合法，提交时拒绝。"""


class LimitExceededError(WorkflowError):
    """超出配置的资源上限。"""


class AccessError(WorkflowError):
    """运行不存在或越权访问。"""
