"""成功步骤不残留历史失败描述 的回归测试。

背景：步骤某次尝试失败时会把失败描述写入 step.error；若之后重试成功，
旧描述必须被清掉——对外查询只要看到步骤最终是成功状态，就不允许再挂着
任何一次历史尝试的失败信息。覆盖两条路径：
- 单次运行内：先失败、重试之后成功（含步骤超时后重试成功）；
- 续跑轮次内：上一轮失败，续跑中先失败、重试之后成功；
  以及上一轮被上游搁置（SKIPPED）、续跑被带起成功的下游。

同时守护反向行为：真正没跑成的步骤（重试耗尽 FAILED、被上游拦住 SKIPPED）
其未完成描述必须照旧保留，不能被一并抹掉。

每个用例通过 logging 打印输入与判定依据；全程不访问网络。
"""
from __future__ import annotations

import logging
import time

from app.actions import register
from app.executor import Executor
from app.models import RunStatus, StepStatus
from app.service import WorkflowService

log = logging.getLogger("stale-error-test")


def _svc(**kwargs) -> WorkflowService:
    exec_kwargs = kwargs.pop("executor_kwargs", {})
    return WorkflowService(executor=Executor(**exec_kwargs), **kwargs)


def _wait_terminal(svc: WorkflowService, run_id: str, timeout: float = 10.0):
    record = svc.wait(run_id, timeout=timeout)
    with record.lock:
        status = record.status
    log.info("判定依据: run %s 终态=%s reason=%s steps=%s",
             run_id[:8], status.value, record.reason,
             {k: (v.status.value, v.error) for k, v in record.steps.items()})
    assert status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                      RunStatus.CANCELLED, RunStatus.TIMED_OUT), "运行未在时限内结束"
    return record


# ---- 1. 单次运行内：先失败、重试后成功，成功步骤不得残留旧失败描述 -----------

def test_retry_success_within_single_run_clears_stale_error():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "flaky", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 2}, "max_retries": 3}]}
    log.info("输入: %s（前 2 次尝试失败、第 3 次成功）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    step = rec.steps["flaky"]
    log.info("判定依据: status=%s attempts=%d error=%r（最终成功则 error 必须为 None）",
             step.status.value, step.attempts, step.error)
    assert rec.status == RunStatus.SUCCEEDED
    assert step.status == StepStatus.SUCCEEDED
    assert step.attempts == 3
    assert step.error is None, "成功步骤不得残留历史尝试的失败描述"


def test_step_timeout_then_retry_success_clears_stale_error():
    def sleep_first_only(params):
        counter = params["_counter"]
        if counter["n"] == 0:
            counter["n"] += 1
            time.sleep(0.2)  # 首次尝试超过 timeout_seconds，被判步骤超时
        return {"ran_after": counter["n"]}
    register("sleep_first_only_stale", sleep_first_only)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 10,
                                "backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep_first_only_stale",
         "params": {"_counter": {"n": 0}},
         "timeout_seconds": 0.05, "max_retries": 1}]}
    log.info("输入: %s（首次尝试超时、重试后成功）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    step = rec.steps["slow"]
    log.info("判定依据: status=%s attempts=%d error=%r（超时后重试成功，"
             "error 必须为 None）", step.status.value, step.attempts, step.error)
    assert rec.status == RunStatus.SUCCEEDED
    assert step.status == StepStatus.SUCCEEDED
    assert step.attempts == 2
    assert step.error is None, "超时重试成功后不得残留 step_timeout 描述"


# ---- 2. 续跑轮次内：先失败、重试后成功；被搁置的下游被带起后同样干净 ---------

def test_resume_round_retry_success_clears_stale_error():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    # flaky 计数器跨轮共享：fail_times=3、max_retries=1
    # 首轮：第 1、2 次尝试失败 -> FAILED；续跑轮次内：第 3 次失败、重试第 4 次成功
    raw = {"workflow_id": "w", "steps": [
        {"id": "up", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 3}, "max_retries": 1},
        {"id": "down", "action": "echo", "params": {"value": "ok"},
         "depends_on": ["up"]}]}
    log.info("输入: %s", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["up"].status == StepStatus.FAILED
    assert rec.steps["up"].error is not None, "真正失败的步骤必须保留失败描述"
    assert rec.steps["down"].status == StepStatus.SKIPPED
    assert rec.steps["down"].error is not None, "被搁置的步骤必须保留搁置原因"
    log.info("首轮: up=FAILED error=%r down=SKIPPED error=%r（未完成信息照旧保留）",
             rec.steps["up"].error, rec.steps["down"].error)

    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    up, down = rec.steps["up"], rec.steps["down"]
    log.info("判定依据: 续跑后 run=%s up=(%s, attempts=%d, error=%r) "
             "down=(%s, error=%r)",
             rec.status.value, up.status.value, up.attempts, up.error,
             down.status.value, down.error)
    assert rec.status == RunStatus.SUCCEEDED
    # 续跑轮次内第 3 次尝试失败、重试第 4 次成功：旧失败描述必须清掉
    assert up.status == StepStatus.SUCCEEDED
    assert up.attempts == 2, "续跑轮次内应有一次失败加重试成功"
    assert up.error is None, "续跑补跑成功后不得残留本轮或上一轮的失败描述"
    # 上一轮被搁置、续跑被带起成功的下游：干净呈现为最终成功
    assert down.status == StepStatus.SUCCEEDED
    assert down.error is None, "被带起成功的下游不得残留旧的搁置描述"
    # 运行整体成功时，所有步骤都不允许残留任何失败/搁置描述（状态自洽）
    assert all(s.error is None for s in rec.steps.values())
    assert rec.reason is None


# ---- 3. 反向守护：真正没跑成的步骤，未完成描述照旧保留 -----------------------

def test_genuinely_unfinished_steps_keep_their_error():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "ok", "action": "echo", "params": {"value": 1}},
        {"id": "bad", "action": "fail",
         "params": {"kind": "retryable"}, "max_retries": 1},
        {"id": "blocked", "action": "echo", "depends_on": ["bad"]}]}
    log.info("输入: %s（bad 重试耗尽仍失败，blocked 被上游拦住）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    ok, bad, blocked = rec.steps["ok"], rec.steps["bad"], rec.steps["blocked"]
    log.info("判定依据: ok.error=%r bad=(%s, error=%r) blocked=(%s, error=%r)",
             ok.error, bad.status.value, bad.error,
             blocked.status.value, blocked.error)
    assert rec.status == RunStatus.FAILED
    assert ok.status == StepStatus.SUCCEEDED and ok.error is None
    assert bad.status == StepStatus.FAILED
    assert bad.error is not None and "可重试失败" in bad.error
    assert blocked.status == StepStatus.SKIPPED
    assert blocked.error is not None and "跳过" in blocked.error
