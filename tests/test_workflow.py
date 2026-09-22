"""核心场景测试：循环依赖、失败重试、并发取消、越权访问、资源超限。

每个用例都通过 logging 打印输入与判定依据；全程不访问网络。
"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from app.errors import (
    CyclicDependencyError,
    DuplicateStepIdError,
    MissingDependencyError,
    ResourceLimitError,
    RunForbiddenError,
    RunNotFoundError,
)
from app.executor import Executor
from app.models import RunStatus, StepStatus
from app.service import WorkflowService
from app.validation import parse_definition, validate_definition

log = logging.getLogger("workflow-test")


def _svc(**kwargs) -> WorkflowService:
    exec_kwargs = kwargs.pop("executor_kwargs", {})
    return WorkflowService(executor=Executor(**exec_kwargs), **kwargs)


def _wait_terminal(svc: WorkflowService, run_id: str, timeout: float = 10.0):
    record = svc.wait(run_id, timeout=timeout)
    with record.lock:
        status = record.status
    log.info("判定依据: run %s 终态=%s reason=%s steps=%s",
             run_id[:8], status.value, record.reason,
             {k: v.status.value for k, v in record.steps.items()})
    assert status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                      RunStatus.CANCELLED, RunStatus.TIMED_OUT), "运行未在时限内结束"
    return record


# ---- 1. 静态校验：循环依赖 / 缺失依赖 / 重复标识 --------------------------

def test_cycle_dependency_rejected():
    raw = {"workflow_id": "w", "steps": [
        {"id": "a", "action": "echo", "depends_on": ["c"]},
        {"id": "b", "action": "echo", "depends_on": ["a"]},
        {"id": "c", "action": "echo", "depends_on": ["b"]},
    ]}
    log.info("输入: %s", raw)
    with pytest.raises(CyclicDependencyError) as exc:
        validate_definition(parse_definition(raw))
    log.info("判定依据: 捕获 %s - %s", type(exc.value).__name__, exc.value)


def test_missing_and_duplicate_rejected():
    missing = {"workflow_id": "w", "steps": [
        {"id": "a", "action": "echo", "depends_on": ["ghost"]}]}
    dup = {"workflow_id": "w", "steps": [
        {"id": "a", "action": "echo"}, {"id": "a", "action": "echo"}]}
    log.info("输入(missing): %s", missing)
    with pytest.raises(MissingDependencyError):
        validate_definition(parse_definition(missing))
    log.info("输入(duplicate): %s", dup)
    with pytest.raises(DuplicateStepIdError):
        validate_definition(parse_definition(dup))


def test_invalid_definition_leaves_no_run_record():
    svc = _svc()
    before = len(svc._runs)
    with pytest.raises(CyclicDependencyError):
        svc.submit("alice", {"workflow_id": "w", "steps": [
            {"id": "a", "action": "echo", "depends_on": ["a"]}]})
    log.info("判定依据: 提交前运行数=%d 提交后=%d（不得有半初始化记录）",
             before, len(svc._runs))
    assert len(svc._runs) == before


# ---- 2. 失败重试与下游状态 -------------------------------------------------

def test_retryable_failure_succeeds_after_retries():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "flaky", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 2}, "max_retries": 3}]})
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: attempts=%d（预期 3 = 2 次失败 + 1 次成功）",
             rec.steps["flaky"].attempts)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["flaky"].attempts == 3


def test_retry_exhaustion_and_skip_propagation():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail",
         "params": {"kind": "retryable"}, "max_retries": 2},
        {"id": "down", "action": "echo", "depends_on": ["bad"]},
    ]})
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: bad.attempts=%d（1+2 次重试）down.status=%s（应为 SKIPPED，"
             "与 FAILED 可区分）", rec.steps["bad"].attempts,
             rec.steps["down"].status.value)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["bad"].status == StepStatus.FAILED
    assert rec.steps["bad"].attempts == 3
    assert rec.steps["down"].status == StepStatus.SKIPPED
    assert rec.steps["down"].attempts == 0


def test_idempotent_side_effect_applied_once():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "pay", "action": "record_then_fail", "params": {"key": "order-1"},
         "max_retries": 3, "idempotency_key": "order-1"}]})
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: output=%s（count 必须为 1，重试命中幂等缓存）",
             rec.steps["pay"].output)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["pay"].output["count"] == 1
    assert rec.steps["pay"].output.get("idempotent_hit") is True


# ---- 3. 并发与取消 ---------------------------------------------------------

def test_parallel_execution_respects_concurrency():
    svc = _svc(executor_kwargs={"max_concurrency": 2, "run_timeout_seconds": 10})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": f"s{i}", "action": "sleep", "params": {"seconds": 0.1}}
        for i in range(4)]})
    start = time.monotonic()
    rec = _wait_terminal(svc, rid)
    elapsed = time.monotonic() - start
    log.info("判定依据: 4 个 0.1s 步骤、并发度=2，耗时 %.3fs 应 >= 0.2s "
             "（两批串行）且远小于 0.4s（全串行）", elapsed)
    assert rec.status == RunStatus.SUCCEEDED
    assert 0.18 <= elapsed < 0.4


def test_concurrent_cancel_leaves_no_background_work():
    svc = _svc(executor_kwargs={"run_timeout_seconds": 30})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.3}},
        {"id": "down", "action": "echo", "depends_on": ["slow"]}]})
    time.sleep(0.05)
    log.info("输入: 运行进行中并发发起 3 次取消 + 5 次查询")
    errors: list[Exception] = []

    def cancel():
        try:
            svc.cancel("alice", rid)
        except Exception as exc:  # 并发取消只允许一个成功，其余为 RunConflictError
            errors.append(exc)

    threads = [threading.Thread(target=cancel) for _ in range(3)]
    threads += [threading.Thread(target=svc.get_run, args=("alice", rid))
                for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rec = svc.get_run("alice", rid)
    alive = svc._threads[rid].is_alive()
    log.info("判定依据: 取消后状态=%s 执行线程存活=%s 并发取消异常=%s",
             rec.status.value, alive, [type(e).__name__ for e in errors])
    assert rec.status == RunStatus.CANCELLED
    assert not alive, "取消后不得残留后台推进的任务"
    assert all(r.status in (StepStatus.CANCELLED, StepStatus.SUCCEEDED)
               for r in rec.steps.values()), "不得有半更新状态"
    assert len(errors) == 2, "3 次并发取消中恰有 1 次成功、2 次冲突"


# ---- 4. 越权访问 -----------------------------------------------------------

def test_forbidden_distinguishable_from_not_found():
    svc = _svc()
    rid = svc.submit("alice", {"workflow_id": "w",
                               "steps": [{"id": "a", "action": "echo"}]})
    _wait_terminal(svc, rid)
    log.info("输入: owner=alice 的运行 %s，由 bob 读取；另读取不存在的 run",
             rid[:8])
    with pytest.raises(RunForbiddenError) as forbidden:
        svc.get_run("bob", rid)
    with pytest.raises(RunNotFoundError) as not_found:
        svc.get_run("bob", "does-not-exist")
    log.info("判定依据: 越权 code=%s，不存在 code=%s（必须可区分）",
             forbidden.value.code, not_found.value.code)
    assert forbidden.value.code != not_found.value.code
    with pytest.raises(RunForbiddenError):
        svc.cancel("bob", rid)


def test_idempotent_submit_returns_same_run_without_reexecution():
    from app.actions import call_counts
    svc = _svc()
    call_counts.clear()
    raw = {"workflow_id": "w", "steps": [{"id": "a", "action": "echo"}]}
    rid1 = svc.submit("alice", raw, idempotency_key="req-42")
    svc.wait(rid1)
    rid2 = svc.submit("alice", raw, idempotency_key="req-42")
    log.info("判定依据: 首次 run=%s 重复提交 run=%s echo 实际调用次数=%d",
             rid1[:8], rid2[:8], call_counts.get("echo"))
    assert rid1 == rid2
    assert call_counts.get("echo") == 1, "重复提交不得产生第二次执行"


# ---- 5. 资源超限 -----------------------------------------------------------

def test_step_count_limit():
    svc = _svc(max_steps_per_run=3)
    raw = {"workflow_id": "w", "steps": [
        {"id": f"s{i}", "action": "echo"} for i in range(4)]}
    log.info("输入: 4 个步骤，上限=3")
    with pytest.raises(ResourceLimitError) as exc:
        svc.submit("alice", raw)
    log.info("判定依据: code=%s message=%s，且运行数=%d（未产生记录）",
             exc.value.code, exc.value, len(svc._runs))
    assert len(svc._runs) == 0


def test_run_timeout_terminates_with_reason():
    svc = _svc(executor_kwargs={"run_timeout_seconds": 0.1})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.5}},
        {"id": "down", "action": "echo", "depends_on": ["slow"]}]})
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: status=%s reason=%s（必须明确说明超时终止，"
             "不得看似成功）", rec.status.value, rec.reason)
    assert rec.status == RunStatus.TIMED_OUT
    assert "超时" in rec.reason
    assert not svc._threads[rid].is_alive()


def test_step_timeout_marks_step_timed_out():
    svc = _svc(executor_kwargs={"run_timeout_seconds": 10})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.3},
         "timeout_seconds": 0.05}]})
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: slow.status=%s error=%s",
             rec.steps["slow"].status.value, rec.steps["slow"].error)
    assert rec.steps["slow"].status == StepStatus.TIMED_OUT
    assert rec.status == RunStatus.FAILED
