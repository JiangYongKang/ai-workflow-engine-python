"""API 测试：越权访问、幂等提交、校验拒绝无半初始化记录、取消接口、错误归一化。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import EngineConfig
from app.service import create_app
from tests.conftest import log_case

ALICE = {"X-Owner": "alice"}
BOB = {"X-Owner": "bob"}


@pytest.fixture()
def client():
    app = create_app(EngineConfig(retry_backoff_base=0.01, max_run_seconds=10.0))
    with TestClient(app) as c:
        yield c


def _ok_workflow():
    return {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo", "params": {"value": 1}},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]}]}


def test_unauthorized_vs_not_found(client):
    r = client.post("/runs", headers=ALICE,
                    json={"idempotency_key": "k1", "wait": True, "workflow": _ok_workflow()})
    run_id = r.json()["run_id"]
    log_case("越权访问", {"run_id": run_id, "owner": "alice", "accessor": "bob"},
             "bob 读取 alice 的运行应得到 403/FORBIDDEN；读取不存在的 id 应得到 404/RUN_NOT_FOUND；"
             "两者必须可区分")
    r_forbidden = client.get(f"/runs/{run_id}", headers=BOB)
    r_missing = client.get("/runs/does-not-exist", headers=BOB)
    assert r_forbidden.status_code == 403
    assert r_forbidden.json()["error"]["code"] == "FORBIDDEN"
    assert r_missing.status_code == 404
    assert r_missing.json()["error"]["code"] == "RUN_NOT_FOUND"
    # 越权取消同样被拒绝
    r_cancel = client.post(f"/runs/{run_id}/cancel", headers=BOB)
    assert r_cancel.status_code == 403


def test_idempotent_resubmit_returns_same_run(client):
    log_case("幂等重复提交", {"idempotency_key": "k2", "提交次数": 2},
             "同一 owner + 同一幂等键重复提交不得产生第二次执行，返回的 run_id 必须与首次一致")
    r1 = client.post("/runs", headers=ALICE,
                     json={"idempotency_key": "k2", "wait": True, "workflow": _ok_workflow()})
    r2 = client.post("/runs", headers=ALICE,
                     json={"idempotency_key": "k2", "wait": True, "workflow": _ok_workflow()})
    assert r1.json()["run_id"] == r2.json()["run_id"]
    assert r2.json()["status"] == "SUCCEEDED"
    # 不同 owner 使用同一键互不影响
    r3 = client.post("/runs", headers=BOB,
                     json={"idempotency_key": "k2", "wait": True, "workflow": _ok_workflow()})
    assert r3.json()["run_id"] != r1.json()["run_id"]


def test_invalid_definition_rejected_without_residue(client):
    bad = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "echo", "depends_on": ["b"]},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]}]}
    log_case("循环依赖拒绝", bad["steps"],
             "提交即拒绝：400/CYCLE_DETECTED；拒绝后不得留下半初始化的运行记录，"
             "用同一幂等键改交合法定义应能正常执行")
    r = client.post("/runs", headers=ALICE,
                    json={"idempotency_key": "k3", "workflow": bad})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "CYCLE_DETECTED"
    # 同一幂等键重新提交合法定义 -> 应作为新提交被接受（拒绝未占用该键）
    r2 = client.post("/runs", headers=ALICE,
                     json={"idempotency_key": "k3", "wait": True, "workflow": _ok_workflow()})
    assert r2.status_code == 201
    assert r2.json()["status"] == "SUCCEEDED"


def test_step_limit_via_api(client):
    app = create_app(EngineConfig(max_steps_per_run=2, retry_backoff_base=0.01))
    with TestClient(app) as c:
        raw = {"workflow_id": "w", "steps": [
            {"step_id": f"s{i}", "action": "echo"} for i in range(3)]}
        log_case("步骤数超限(API)", {"steps": 3, "max_steps_per_run": 2},
                 "超限必须拒绝并给出 STEP_LIMIT_EXCEEDED，不得静默截断")
        r = c.post("/runs", headers=ALICE, json={"idempotency_key": "k4", "workflow": raw})
        assert r.status_code == 400
        assert r.json()["error"]["code"] == "STEP_LIMIT_EXCEEDED"


def test_cancel_via_api(client):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "slow", "params": {"duration": 5}}]}
    log_case("取消接口", raw["steps"],
             "运行中调用 cancel，最终状态应为 CANCELLED/RUN_CANCELLED，且查询接口可见一致状态")
    r = client.post("/runs", headers=ALICE, json={"idempotency_key": "k5", "workflow": raw})
    run_id = r.json()["run_id"]
    client.post(f"/runs/{run_id}/cancel", headers=ALICE)
    import time
    for _ in range(200):
        view = client.get(f"/runs/{run_id}", headers=ALICE).json()
        if view["terminal"]:
            break
        time.sleep(0.02)
    assert view["status"] == "CANCELLED"
    assert view["error_code"] == "RUN_CANCELLED"
    assert view["steps"]["a"]["status"] == "CANCELLED"


def test_internal_error_not_leaked(client):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "unknown-action-xyz"}]}
    log_case("错误归一化", raw["steps"],
             "未知动作的执行错误应归一化为带 error_code 的结构，响应中不得出现底层异常类名/堆栈")
    r = client.post("/runs", headers=ALICE,
                    json={"idempotency_key": "k6", "wait": True, "workflow": raw})
    body = r.json()
    assert body["steps"]["a"]["status"] == "FAILED"
    assert body["steps"]["a"]["error_code"] == "INVALID_DEFINITION"
    assert "Traceback" not in str(body) and "KeyError" not in str(body)
