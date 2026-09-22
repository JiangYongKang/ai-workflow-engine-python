"""错误分类：对外只暴露这些可归一化的错误类型（骨架）。"""
from __future__ import annotations


class WorkflowError(Exception):
    """所有对外错误的基类。"""
    code = "internal_error"


class DefinitionError(WorkflowError):
    """工作流定义不合法（结构/重复标识）。"""
    code = "invalid_definition"


class CyclicDependencyError(DefinitionError):
    code = "cyclic_dependency"


class MissingDependencyError(DefinitionError):
    code = "missing_dependency"


class DuplicateStepIdError(DefinitionError):
    code = "duplicate_step_id"


class RunNotFoundError(WorkflowError):
    code = "run_not_found"


class RunForbiddenError(WorkflowError):
    """运行存在但不属于当前调用者。"""
    code = "run_forbidden"


class RunConflictError(WorkflowError):
    """运行已处于终态，无法执行该操作。"""
    code = "run_conflict"


class StepTimeoutError(WorkflowError):
    code = "step_timeout"


class RetryableStepError(WorkflowError):
    code = "retryable_step_error"


class FatalStepError(WorkflowError):
    code = "fatal_step_error"


class ResourceLimitError(WorkflowError):
    code = "resource_limit_exceeded"
