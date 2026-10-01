"""成功步骤对外不再残留历史失败描述 的回归测试。

覆盖：
- 单次运行内：步骤先失败、重试后成功 -> 查询时不得带之前尝试的失败描述；
- 续跑轮次内：上一轮失败，续跑中先失败、重试后成功 -> 同样不得残留；
- 对照组：真正没跑成的步骤（不可重试失败、被上游拦住 SKIPPED）的
  未完成说明必须照旧保留，且在续跑后依然保留；
- 下游因上游补齐而被带起来跑成功的，也要干净呈现为最终成功。

每个用例通过 logging 打印输入与判定依据；全程不访问网络。
"""
from __future__ import annotations

import logging

from app.executor import Executor
from app.models import RunStatus, StepStatus
from app.service import WorkflowService

log = logging.getLogger("success-clears-error-test")


def _svc(**executor_kwargs) -> WorkflowService:
    executor_kwargs.setdefault("backoff_base_seconds", 0.01)
    return WorkflowService(executor=Executor(**executor_kwargs))


def _wait_terminal(svc: WorkflowService, run_id: str, timeout: float = 10.0):
    record = svc.wait(run_id, timeout=timeout)
    with record.lock:
        status = record.status
        snapshot = {k: (v.status.value, v.error) for k, v in record.steps.items()}
    log.info("判定依据: run %s 终态=%s reason=%s steps=%s",
             run_id[:8], status.value, record.reason, snapshot)
    assert status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                      RunStatus.CANCELLED, RunStatus.TIMED_OUT), "运行未在时限内结束"
    return record


# ---- 1. 单次运行内先失败、重试后成功：不得残留旧失败描述 ---------------------

def test_retry_success_within_single_run_clears_stale_error():
    svc = _svc()
    raw = {"workflow_id": "w", "steps": [
        # flaky 第 1 次尝试失败（可重试），第 2 次成功；给 1 次重试预算
        {"id": "flaky", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 1},
         "max_retries": 1},
        {"id": "down", "action": "echo",
         "params": {"value": "ok"}, "depends_on": ["flaky"]}]}
    log.info("输入: %s（flaky 先失败 1 次再成功，同一次运行内重试）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)

    log.info("判定依据: status=%s flaky.attempts=%d（=2，先失败后重试成功）"
             "flaky.error=%r（必须为 None）down.error=%r",
             rec.status.value, rec.steps["flaky"].attempts,
             rec.steps["flaky"].error, rec.steps["down"].error)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["flaky"].status == StepStatus.SUCCEEDED
    assert rec.steps["flaky"].attempts == 2, "必须是先失败、重试后才成功"
    assert rec.steps["flaky"].error is None, (
        "最终成功的步骤不得残留上一次尝试的失败描述")
    assert rec.steps["flaky"].output == {"attempts": 2}
    assert rec.steps["down"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].error is None


# ---- 2. 续跑轮次内先失败、重试后成功：不得残留任何旧失败描述 ------------------

def test_retry_success_within_resume_round_clears_stale_error():
    svc = _svc()
    raw = {"workflow_id": "w", "steps": [
        # flaky 前 3 次失败、第 4 次成功；每轮只有 1 次重试（每轮 2 次尝试）
        # -> 首轮 2 次尝试后 FAILED；续跑轮第 1 次尝试失败、重试后成功
        {"id": "up", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 3},
         "max_retries": 1},
        {"id": "down", "action": "echo",
         "params": {"value": "ok"}, "depends_on": ["up"]}]}
    log.info("输入: %s（首轮失败；续跑轮内先失败、重试后成功）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["up"].status == StepStatus.FAILED
    assert rec.steps["up"].error is not None, "首轮失败必须留下失败描述"
    assert rec.steps["down"].status == StepStatus.SKIPPED
    assert rec.steps["down"].error is not None, "首轮被搁置必须留下说明"
    log.info("首轮: up.error=%r down.error=%r（失败/搁置描述此时应当存在）",
             rec.steps["up"].error, rec.steps["down"].error)

    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s up.attempts=%d（=2，续跑轮内先失败后"
             "重试成功）up.error=%r down.status=%s down.error=%r",
             rec.status.value, rec.steps["up"].attempts,
             rec.steps["up"].error, rec.steps["down"].status.value,
             rec.steps["down"].error)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["up"].status == StepStatus.SUCCEEDED
    assert rec.steps["up"].attempts == 2, "续跑轮内必须是先失败、重试后才成功"
    assert rec.steps["up"].error is None, (
        "续跑补跑成功后不得残留本轮或上一轮的失败描述")
    # 下游因上游补齐被带起来跑成功，也要干净呈现为最终成功
    assert rec.steps["down"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].error is None, (
        "被带起来跑成功的下游不得残留上一轮被搁置的说明")
    assert rec.steps["down"].output == {"echo": "ok"}


# ---- 3. 对照组：真正没跑成的步骤，未完成说明照旧保留 -------------------------

def test_unfinished_steps_keep_their_error_after_fix():
    svc = _svc()
    raw = {"workflow_id": "w", "steps": [
        # ok：先失败 1 次再重试成功（最终成功 -> 不留旧描述）
        {"id": "ok", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 1},
         "max_retries": 1},
        # bad：不可重试失败（终态 FAILED -> 说明必须保留）
        {"id": "bad", "action": "fail",
         "params": {"kind": "fatal", "message": "永久失败"}},
        # blocked：被 bad 拦住未执行（SKIPPED -> 说明必须保留）
        {"id": "blocked", "action": "echo",
         "params": {"value": 1}, "depends_on": ["bad"]}]}
    log.info("输入: %s（ok 重试后成功；bad 永久失败；blocked 被上游拦住）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)

    log.info("判定依据: status=%s ok.error=%r（None）bad.error=%r（保留）"
             "blocked.error=%r（保留）",
             rec.status.value, rec.steps["ok"].error,
             rec.steps["bad"].error, rec.steps["blocked"].error)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["ok"].status == StepStatus.SUCCEEDED
    assert rec.steps["ok"].error is None, "成功步骤不得残留旧失败描述"
    assert rec.steps["bad"].status == StepStatus.FAILED
    assert rec.steps["bad"].error is not None
    assert "永久失败" in rec.steps["bad"].error, "失败步骤的说明必须照旧保留"
    assert rec.steps["blocked"].status == StepStatus.SKIPPED
    assert rec.steps["blocked"].error == "上游未成功，步骤被跳过"

    # 续跑一轮后 bad 仍失败：未完成说明依旧保留，ok 依旧干净
    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s ok.error=%r bad.error=%r blocked.error=%r",
             rec.status.value, rec.steps["ok"].error,
             rec.steps["bad"].error, rec.steps["blocked"].error)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["ok"].status == StepStatus.SUCCEEDED
    assert rec.steps["ok"].error is None
    assert rec.steps["bad"].status == StepStatus.FAILED
    assert rec.steps["bad"].error is not None
    assert "永久失败" in rec.steps["bad"].error
    assert rec.steps["blocked"].status == StepStatus.SKIPPED
    assert rec.steps["blocked"].error == "上游未成功，步骤被跳过"
