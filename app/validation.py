"""工作流定义的解析与静态校验。

校验规则（全部在执行开始前完成，失败即拒绝，不产生运行记录）：
- 结构不合法           -> DefinitionError
- 步骤标识重复         -> DuplicateStepIdError
- 依赖不存在的步骤     -> MissingDependencyError
- 依赖构成环           -> CyclicDependencyError
"""
from __future__ import annotations

from typing import Any

from app.errors import (
    CyclicDependencyError,
    DefinitionError,
    DuplicateStepIdError,
    MissingDependencyError,
)
from app.models import StepDefinition, WorkflowDefinition

_MAX_ID_LEN = 128


def _require(cond: bool, message: str) -> None:
    if not cond:
        raise DefinitionError(message)


def _parse_step(raw: Any, index: int) -> StepDefinition:
    _require(isinstance(raw, dict), f"steps[{index}] 必须是对象")
    step_id = raw.get("id")
    _require(isinstance(step_id, str) and 0 < len(step_id) <= _MAX_ID_LEN,
             f"steps[{index}].id 必须是 1..{_MAX_ID_LEN} 字符的字符串")
    action = raw.get("action")
    _require(isinstance(action, str) and len(action) > 0,
             f"步骤 {step_id!r} 缺少合法的 action")
    params = raw.get("params", {})
    _require(isinstance(params, dict), f"步骤 {step_id!r} 的 params 必须是对象")
    depends_on = raw.get("depends_on", [])
    _require(isinstance(depends_on, list)
             and all(isinstance(d, str) for d in depends_on),
             f"步骤 {step_id!r} 的 depends_on 必须是字符串数组")
    timeout = raw.get("timeout_seconds")
    _require(timeout is None or (isinstance(timeout, (int, float)) and timeout > 0),
             f"步骤 {step_id!r} 的 timeout_seconds 必须是正数")
    max_retries = raw.get("max_retries", 0)
    _require(isinstance(max_retries, int) and max_retries >= 0,
             f"步骤 {step_id!r} 的 max_retries 必须是非负整数")
    idem = raw.get("idempotency_key")
    _require(idem is None or isinstance(idem, str),
             f"步骤 {step_id!r} 的 idempotency_key 必须是字符串")
    return StepDefinition(
        step_id=step_id,
        action=action,
        params=dict(params),
        depends_on=tuple(dict.fromkeys(depends_on)),
        timeout_seconds=float(timeout) if timeout is not None else None,
        max_retries=max_retries,
        idempotency_key=idem,
    )


def parse_definition(raw: dict[str, Any]) -> WorkflowDefinition:
    """把客户端提交的 dict 解析为 WorkflowDefinition，结构不合法时抛 DefinitionError。"""
    _require(isinstance(raw, dict), "工作流定义必须是对象")
    workflow_id = raw.get("workflow_id", "workflow")
    _require(isinstance(workflow_id, str) and 0 < len(workflow_id) <= _MAX_ID_LEN,
             "workflow_id 必须是合法字符串")
    steps_raw = raw.get("steps")
    _require(isinstance(steps_raw, list) and len(steps_raw) > 0,
             "steps 必须是非空数组")
    steps = tuple(_parse_step(s, i) for i, s in enumerate(steps_raw))
    return WorkflowDefinition(workflow_id=workflow_id, steps=steps)


def validate_definition(defn: WorkflowDefinition) -> None:
    """静态校验：重复标识、依赖不存在、循环依赖。"""
    seen: set[str] = set()
    for step in defn.steps:
        if step.step_id in seen:
            raise DuplicateStepIdError(f"步骤标识重复: {step.step_id!r}")
        seen.add(step.step_id)

    for step in defn.steps:
        for dep in step.depends_on:
            if dep not in seen:
                raise MissingDependencyError(
                    f"步骤 {step.step_id!r} 依赖不存在的步骤 {dep!r}")

    # 拓扑排序检测环（Kahn 算法）
    indegree = {s.step_id: 0 for s in defn.steps}
    for step in defn.steps:
        for dep in step.depends_on:
            indegree[step.step_id] += 1
    queue = [sid for sid, d in indegree.items() if d == 0]
    visited = 0
    dependents: dict[str, list[str]] = {s.step_id: [] for s in defn.steps}
    for step in defn.steps:
        for dep in step.depends_on:
            dependents[dep].append(step.step_id)
    while queue:
        node = queue.pop()
        visited += 1
        for nxt in dependents[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if visited != len(defn.steps):
        remaining = sorted(sid for sid, d in indegree.items() if d > 0)
        raise CyclicDependencyError(f"检测到循环依赖，涉及步骤: {remaining}")
