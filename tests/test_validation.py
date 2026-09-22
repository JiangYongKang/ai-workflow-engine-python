"""静态校验测试：循环依赖、依赖缺失、重复标识、结构不合法、步骤数超限。"""
from __future__ import annotations

import pytest

from app.errors import ErrorCode, WorkflowError
from app.validation import parse_definition, validate_definition
from tests.conftest import log_case


def _expect(raw, code: ErrorCode, max_steps: int = 64):
    try:
        validate_definition(parse_definition(raw), max_steps)
    except WorkflowError as e:
        return e.code
    return None


def test_cycle_dependency_rejected():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo", "depends_on": ["b"]},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]},
    ]}
    log_case("循环依赖", raw, "a<->b 构成环，Kahn 拓扑排序无法消费全部节点，应拒绝为 CYCLE_DETECTED")
    assert _expect(raw, ErrorCode.CYCLE_DETECTED) == ErrorCode.CYCLE_DETECTED


def test_self_cycle_rejected():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo", "depends_on": ["a"]}]}
    log_case("自环", raw, "步骤依赖自身也是环，应拒绝为 CYCLE_DETECTED")
    assert _expect(raw, ErrorCode.CYCLE_DETECTED) == ErrorCode.CYCLE_DETECTED


def test_missing_dependency_rejected():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo", "depends_on": ["ghost"]}]}
    log_case("依赖不存在", raw, "ghost 未在 steps 中定义，应拒绝为 MISSING_DEPENDENCY")
    assert _expect(raw, ErrorCode.MISSING_DEPENDENCY) == ErrorCode.MISSING_DEPENDENCY


def test_duplicate_step_id_rejected():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo"},
        {"step_id": "a", "action": "echo"}]}
    log_case("标识重复", raw, "两个步骤同名 a，应拒绝为 DUPLICATE_STEP_ID")
    assert _expect(raw, ErrorCode.DUPLICATE_STEP_ID) == ErrorCode.DUPLICATE_STEP_ID


@pytest.mark.parametrize("raw", [
    {"steps": [{"step_id": "a", "action": "echo"}]},               # 缺 workflow_id
    {"workflow_id": "w", "steps": []},                              # 空步骤
    {"workflow_id": "w", "steps": [{"action": "echo"}]},            # 缺 step_id
    {"workflow_id": "w", "steps": [{"step_id": "a"}]},              # 缺 action
    {"workflow_id": "w", "steps": [{"step_id": "a", "action": "echo", "depends_on": "x"}]},
    {"workflow_id": "w", "steps": [{"step_id": "a", "action": "echo", "timeout_seconds": -1}]},
])
def test_invalid_structure_rejected(raw):
    log_case("结构不合法", raw, "必填字段缺失或类型错误，应拒绝为 INVALID_DEFINITION")
    assert _expect(raw, ErrorCode.INVALID_DEFINITION) == ErrorCode.INVALID_DEFINITION


def test_step_limit_exceeded():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": f"s{i}", "action": "echo"} for i in range(5)]}
    log_case("步骤数超限", {"steps": 5, "max_steps": 3},
             "5 步超过上限 3，应拒绝为 STEP_LIMIT_EXCEEDED 且不得静默截断")
    assert _expect(raw, ErrorCode.STEP_LIMIT_EXCEEDED, max_steps=3) == ErrorCode.STEP_LIMIT_EXCEEDED


def test_valid_diamond_accepted():
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo"},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]},
        {"step_id": "c", "action": "echo", "depends_on": ["a"]},
        {"step_id": "d", "action": "echo", "depends_on": ["b", "c"]},
    ]}
    log_case("合法钻石 DAG", raw, "无环、依赖齐全、标识唯一，应通过校验")
    validate_definition(parse_definition(raw), 64)  # 不抛异常即通过
