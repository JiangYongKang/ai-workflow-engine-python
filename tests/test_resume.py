"""失败续跑（resume）场景测试。

覆盖：
- 失败后续跑最终成功、上一轮被搁置（SKIPPED）的下游被正常带起来；
- 已成功步骤沿用上一次结果、不重复执行；幂等副作用跨续跑不产生第二次效果；
- 运行超时进入 TIMED_OUT 后续跑成功；
- 重复续跑（同幂等键）只生效一次且返回一致的 resume_id；
- 并发续跑只允许一个成功，其余得到 run_conflict（与 404/403 可区分）；
- 运行未结束 / 已成功 / 已取消时续跑被明确拒绝；
- 续跑次数可配置上限，超限拒绝并说明原因，不悄悄成功、状态不被改写；
- 续跑结束（含续跑中取消）后后台线程回收、无半更新状态。

每个用例通过 logging 打印输入与判定依据；全程本地动作，不访问网络。
"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from app.actions import call_counts, register
from app.errors import (
    ResumeLimitError,
    RunConflictError,
    RunForbiddenError,
    RunNotFoundError,
)
from app.executor import Executor
from app.models import RunStatus, StepStatus
from app.service import WorkflowService

log = logging.getLogger("workflow-test")


def _svc(**kwargs) -> WorkflowService:
    exec_kwargs = kwargs.pop("executor_kwargs", {})
    return WorkflowService(executor=Executor(**exec_kwargs), **kwargs)


def _wait_terminal(svc: WorkflowService, run_id: str, timeout: float = 10.0):
    record = svc.wait(run_id, timeout=timeout)
    with record.lock:
        status = record.status
    assert status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                      RunStatus.CANCELLED, RunStatus.TIMED_OUT), "运行未在时限内结束"
    return record


# 测试专用动作：第一次调用很慢（用于触发运行级超时），之后立即返回
_slow_state = {"calls": 0}


def _action_slow_then_fast(params):
    _slow_state["calls"] += 1
    n = _slow_state["calls"]
    if n == 1:
        time.sleep(float(params.get("slow_seconds", 0.3)))
    return {"calls": n}


register("slow_then_fast", _action_slow_then_fast)


# ---- 1. 失败后续跑成功：搁置下游被带起、成功步骤不重跑 --------------------

def test_resume_after_failure_succeeds_and_runs_skipped_downstream():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        # good：首轮成功，续跑时必须沿用结果、不得再执行一次
        {"id": "good", "action": "record", "params": {"key": "order-9"}},
        # bad：首轮失败（可重试错误但不给重试次数）；其第二次调用会成功
        {"id": "bad", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 1}, "max_retries": 0},
        # down：首轮因 bad 失败被搁置；续跑补齐 bad 后必须被正常带起来
        {"id": "down", "action": "echo",
         "params": {"value": "after"}, "depends_on": ["bad", "good"]},
    ]}
    log.info("输入: %s（预期首轮 good 成功 / bad 失败 / down 搁置）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据(首轮): status=%s steps=%s good.output=%s",
             rec.status.value,
             {k: v.status.value for k, v in rec.steps.items()},
             rec.steps["good"].output)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["good"].status == StepStatus.SUCCEEDED
    assert rec.steps["bad"].status == StepStatus.FAILED
    assert rec.steps["down"].status == StepStatus.SKIPPED

    record_calls_before = call_counts.get("record", 0)
    echo_calls_before = call_counts.get("echo", 0)
    resume_id, _ = svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据(续跑): status=%s resume_count=%d steps=%s bad.error=%r "
             "record调用增量=%d echo调用增量=%d",
             rec.status.value, rec.resume_count,
             {k: v.status.value for k, v in rec.steps.items()},
             rec.steps["bad"].error,
             call_counts.get("record", 0) - record_calls_before,
             call_counts.get("echo", 0) - echo_calls_before)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.resume_count == 1
    assert {k: v.status.value for k, v in rec.steps.items()} == {
        "good": "SUCCEEDED", "bad": "SUCCEEDED", "down": "SUCCEEDED"}
    # 已成功步骤沿用旧结果，不重新执行
    assert rec.steps["good"].output == {"recorded": "order-9", "count": 1}
    assert call_counts.get("record", 0) - record_calls_before == 0, \
        "已成功步骤不得在续跑中被重复执行"
    # 被搁置的下游只在续跑中执行这一次
    assert call_counts.get("echo", 0) - echo_calls_before == 1
    # 续跑成功后上一轮失败信息不得残留
    assert rec.steps["bad"].error is None
    assert rec.reason is None
    assert isinstance(resume_id, str) and len(resume_id) > 0


def test_resume_reuses_effect_ledger_side_effect_not_applied_twice():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "pay", "action": "record_then_fail", "params": {"key": "pay-77"},
         "max_retries": 0, "idempotency_key": "pay-77"}]}
    log.info("输入: %s（副作用首轮已生效但步骤失败，续跑必须命中缓存）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    calls_before = call_counts.get("record_then_fail", 0)
    log.info("判定依据(首轮): pay.error=%s 动作实际调用=%d",
             rec.steps["pay"].error, calls_before)

    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    calls_after = call_counts.get("record_then_fail", 0)
    log.info("判定依据(续跑): status=%s output=%s 动作调用增量=%d（必须为 0，"
             "同一逻辑副作用不得产生第二次效果）",
             rec.status.value, rec.steps["pay"].output, calls_after - calls_before)
    assert rec.status == RunStatus.SUCCEEDED
    assert calls_after - calls_before == 0
    assert rec.steps["pay"].output["count"] == 1
    assert rec.steps["pay"].output.get("idempotent_hit") is True


# ---- 2. 超时后续跑 ---------------------------------------------------------

def test_resume_after_timeout_succeeds():
    _slow_state["calls"] = 0
    # 运行总耗时上限 0.15s：首轮慢动作 0.3s 必然把运行拖到 TIMED_OUT
    svc = _svc(executor_kwargs={"run_timeout_seconds": 0.15})
    raw = {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "slow_then_fast",
         "params": {"slow_seconds": 0.3}},
        {"id": "down", "action": "echo", "depends_on": ["slow"]}]}
    log.info("输入: %s（首轮触发运行级超时）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据(首轮): status=%s reason=%s", rec.status.value, rec.reason)
    assert rec.status == RunStatus.TIMED_OUT
    assert not svc._threads[rid].is_alive(), "首轮超时后不得残留后台线程"

    svc.resume("alice", rid)  # 同一动作第二次调用立即返回
    rec = _wait_terminal(svc, rid)
    log.info("判定依据(续跑): status=%s steps=%s thread_alive=%s",
             rec.status.value,
             {k: v.status.value for k, v in rec.steps.items()},
             svc._threads[rid].is_alive())
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["slow"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].status == StepStatus.SUCCEEDED
    assert not svc._threads[rid].is_alive(), "续跑结束后不得残留后台推进的任务"
    assert all(r.status in (StepStatus.SUCCEEDED, StepStatus.FAILED,
                            StepStatus.TIMED_OUT, StepStatus.SKIPPED,
                            StepStatus.CANCELLED)
               for r in rec.steps.values()), "终态后不得有半更新步骤"


# ---- 3. 重复续跑幂等 -------------------------------------------------------

def test_duplicate_resume_same_key_returns_same_id_once():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]})
    _wait_terminal(svc, rid)
    log.info("输入: 对失败运行 %s 用同一幂等键 res-1 连续续跑两次", rid[:8])
    id1, _ = svc.resume("alice", rid, idempotency_key="res-1")
    _wait_terminal(svc, rid)
    id2, rec = svc.resume("alice", rid, idempotency_key="res-1")
    log.info("判定依据: resume_id 两次=%s/%s resume_count=%d（重复请求只生效一次）",
             id1[:8], id2[:8], rec.resume_count)
    assert id1 == id2
    assert rec.resume_count == 1


def test_concurrent_resumes_same_key_single_execution():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]})
    _wait_terminal(svc, rid)
    log.info("输入: 3 个线程以同一幂等键并发续跑运行 %s", rid[:8])
    barrier = threading.Barrier(3)
    ids: list[str] = []
    errors: list[Exception] = []

    def worker():
        barrier.wait()
        try:
            resume_id, _ = svc.resume("alice", rid, idempotency_key="same-key")
            ids.append(resume_id)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _wait_terminal(svc, rid)
    rec = svc.get_run("alice", rid)
    log.info("判定依据: 返回标识集合=%s 异常=%s resume_count=%d",
             {i[:8] for i in ids}, [type(e).__name__ for e in errors],
             rec.resume_count)
    assert errors == []
    assert len(ids) == 3 and len(set(ids)) == 1, "同键并发必须拿到一致的结果标识"
    assert rec.resume_count == 1, "同键并发只允许真正生效一次"


# ---- 4. 并发续跑互斥 -------------------------------------------------------

def test_concurrent_resumes_distinct_requests_only_one_wins():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01},
               max_resumes_per_run=5)
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]})
    _wait_terminal(svc, rid)
    log.info("输入: 5 个线程以不同请求并发续跑同一失败运行 %s", rid[:8])
    barrier = threading.Barrier(5)
    winners: list[str] = []
    conflicts: list[str] = []

    def worker(i: int):
        barrier.wait()
        try:
            resume_id, _ = svc.resume("alice", rid, idempotency_key=f"k-{i}")
            winners.append(resume_id)
        except RunConflictError as exc:
            conflicts.append(exc.code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _wait_terminal(svc, rid)
    log.info("判定依据: 成功续跑数=%d 冲突数=%d 冲突 code 集合=%s",
             len(winners), len(conflicts), set(conflicts))
    assert len(winners) == 1, "并发续跑只允许一个成功"
    assert len(conflicts) == 4
    assert set(conflicts) == {"run_conflict"}


# ---- 5. 非法时机 / 权限 / 不存在：三类拒绝相互可区分 -----------------------

def test_resume_rejected_while_running_and_after_success_or_cancel():
    svc = _svc(executor_kwargs={"run_timeout_seconds": 10})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.3}}]})
    # 等待运行真正进入 RUNNING，避免 PENDING 窗口的偶发性
    for _ in range(100):
        if svc.get_run("alice", rid).status == RunStatus.RUNNING:
            break
        time.sleep(0.005)
    log.info("输入: 运行 %s 处于 RUNNING 时请求续跑", rid[:8])
    with pytest.raises(RunConflictError) as running_exc:
        svc.resume("alice", rid)
    _wait_terminal(svc, rid)
    log.info("判定依据(运行中): code=%s", running_exc.value.code)
    with pytest.raises(RunConflictError) as success_exc:
        svc.resume("alice", rid)  # 已 SUCCEEDED
    log.info("判定依据(成功后): code=%s - %s",
             success_exc.value.code, success_exc.value)

    rid2 = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.3}}]})
    svc.cancel("alice", rid2)
    with pytest.raises(RunConflictError) as cancel_exc:
        svc.resume("alice", rid2)  # 已 CANCELLED 同样不允许续跑
    log.info("判定依据(取消后): code=%s - %s",
             cancel_exc.value.code, cancel_exc.value)


def test_resume_error_categories_are_distinguishable():
    svc = _svc()
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "a", "action": "echo"}]})
    _wait_terminal(svc, rid)
    log.info("输入: bob 续跑 alice 的运行；alice 续跑不存在的运行；"
             "alice 续跑已成功的运行")
    with pytest.raises(RunForbiddenError) as forbidden:
        svc.resume("bob", rid)
    with pytest.raises(RunNotFoundError) as missing:
        svc.resume("alice", "does-not-exist")
    with pytest.raises(RunConflictError) as conflict:
        svc.resume("alice", rid)
    log.info("判定依据: 越权=%s 不存在=%s 冲突=%s（三者必须互不相同）",
             forbidden.value.code, missing.value.code, conflict.value.code)
    assert len({forbidden.value.code, missing.value.code,
                conflict.value.code}) == 3


# ---- 6. 续跑次数上限 -------------------------------------------------------

def test_resume_count_limit_rejected_with_reason():
    svc = _svc(max_resumes_per_run=2)
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]})
    _wait_terminal(svc, rid)
    log.info("输入: max_resumes_per_run=2，对持续失败的运行连续请求第 3 次续跑")
    for expected in (1, 2):
        svc.resume("alice", rid, idempotency_key=f"r-{expected}")
        _wait_terminal(svc, rid)
    rec_before = svc.get_run("alice", rid)
    assert rec_before.resume_count == 2
    with pytest.raises(ResumeLimitError) as exc:
        svc.resume("alice", rid, idempotency_key="r-3")
    rec_after = svc.get_run("alice", rid)
    log.info("判定依据: code=%s message=%s 超限后 resume_count=%d status=%s"
             "（不得悄悄成功、不得无限续跑）",
             exc.value.code, exc.value, rec_after.resume_count,
             rec_after.status.value)
    assert exc.value.code == "resume_limit_exceeded"
    assert "上限" in str(exc.value), "拒绝原因必须说明触顶的上限"
    assert rec_after.resume_count == 2
    assert rec_after.status == RunStatus.FAILED
    assert not svc._threads[rid].is_alive()


# ---- 7. 续跑过程中取消：线程回收、无半更新状态 -----------------------------

def test_cancel_during_resume_leaves_no_background_work():
    _slow_state["calls"] = 0
    svc = _svc(executor_kwargs={"run_timeout_seconds": 0.15})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "slow_then_fast",
         "params": {"slow_seconds": 0.3}}]})
    _wait_terminal(svc, rid)
    assert svc.get_run("alice", rid).status == RunStatus.TIMED_OUT
    log.info("输入: 对 %s 发起续跑后立刻取消", rid[:8])
    svc.resume("alice", rid)
    rec = svc.cancel("alice", rid)  # join 等待执行线程回收
    alive = svc._threads[rid].is_alive()
    log.info("判定依据: status=%s thread_alive=%s steps=%s",
             rec.status.value, alive,
             {k: v.status.value for k, v in rec.steps.items()})
    assert rec.status == RunStatus.CANCELLED
    assert not alive, "续跑取消后不得残留后台推进的任务"
    assert all(r.status in (StepStatus.SUCCEEDED, StepStatus.CANCELLED)
               for r in rec.steps.values()), "终态后不得出现半更新步骤"
