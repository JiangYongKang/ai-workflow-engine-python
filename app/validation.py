"""工作流定义的解析与静态校验（DAG 校验）。"""
from __future__ import annotations

from typing import Any

from .errors import DefinitionError, ErrorCode, LimitExceededError
from .models import StepDefinition, WorkflowDefinition


def parse_definition(raw: Any) -> WorkflowDefinition:
    """把客户端提交的原始 dict 解析为 WorkflowDefinition，结构不合法时抛 DefinitionError。"""
    if not isinstance(raw, dict):
        raise DefinitionError(ErrorCode.INVALID_DEFINITION, "workflow definition must be an object")
    workflow_id = raw.get("workflow_id")
    if not isinstance(workflow_id, str) or not workflow_id.strip():
        raise DefinitionError(ErrorCode.INVALID_DEFINITION, "workflow_id must be a non-empty string")
    raw_steps = raw.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise DefinitionError(ErrorCode.INVALID_DEFINITION, "steps must be a non-empty list")

    steps: list[StepDefinition] = []
    for i, rs in enumerate(raw_steps):
        if not isinstance(rs, dict):
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}] must be an object")
        step_id = rs.get("step_id")
        if not isinstance(step_id, str) or not step_id.strip():
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].step_id must be a non-empty string")
        action = rs.get("action")
        if not isinstance(action, str) or not action.strip():
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].action must be a non-empty string")
        depends_on = rs.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(d, str) for d in depends_on):
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].depends_on must be a list of strings")
        params = rs.get("params", {})
        if not isinstance(params, dict):
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].params must be an object")
        timeout = rs.get("timeout_seconds")
        if timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0):
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].timeout_seconds must be a positive number")
        max_retries = rs.get("max_retries")
        if max_retries is not None and (not isinstance(max_retries, int) or max_retries < 0):
            raise DefinitionError(ErrorCode.INVALID_DEFINITION, f"steps[{i}].max_retries must be a non-negative integer")
        steps.append(StepDefinition(
            step_id=step_id, action=action, depends_on=tuple(depends_on),
            params=params, timeout_seconds=timeout, max_retries=max_retries,
        ))
    return WorkflowDefinition(workflow_id=workflow_id, steps=tuple(steps))


def validate_definition(defn: WorkflowDefinition, max_steps: int) -> None:
    """静态校验：重复标识、依赖缺失、循环依赖、步骤数上限。"""
    if len(defn.steps) > max_steps:
        raise LimitExceededError(
            ErrorCode.STEP_LIMIT_EXCEEDED,
            f"workflow has {len(defn.steps)} steps, limit is {max_steps}",
        )

    seen: set[str] = set()
    for s in defn.steps:
        if s.step_id in seen:
            raise DefinitionError(ErrorCode.DUPLICATE_STEP_ID, f"duplicate step_id: {s.step_id!r}")
        seen.add(s.step_id)

    for s in defn.steps:
        for dep in s.depends_on:
            if dep not in seen:
                raise DefinitionError(
                    ErrorCode.MISSING_DEPENDENCY,
                    f"step {s.step_id!r} depends on unknown step {dep!r}",
                )

    # 拓扑排序检测循环（Kahn 算法）
    indegree: dict[str, int] = {s.step_id: 0 for s in defn.steps}
    dependents: dict[str, list[str]] = {s.step_id: [] for s in defn.steps}
    for s in defn.steps:
        for dep in s.depends_on:
            indegree[s.step_id] += 1
            dependents[dep].append(s.step_id)
    queue = [sid for sid, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for nxt in dependents[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if visited != len(defn.steps):
        remaining = sorted(sid for sid, deg in indegree.items() if deg > 0)
        raise DefinitionError(
            ErrorCode.CYCLE_DETECTED,
            f"dependency cycle detected involving steps: {remaining}",
        )
