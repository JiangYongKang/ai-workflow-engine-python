"""失败续跑（resume）场景测试。

覆盖：
- 失败后续跑最终成功，上一轮因上游失败被搁置（SKIPPED）的下游被带起来；
- 已经成功的步骤续跑时不会被重复执行，逻辑副作用只生效一次；
- 超时终态的运行可以续跑成功；
- 续跑幂等：相同 (提交者, 幂等标识) 重复提交只生效一次、返回一致 resume_id；
- 并发续跑：只有一个成功，其余得到明确的 resume_conflict；
- 运行未结束 / 成功 / 取消后不允许续跑；越权续跑与不存在可区分；
- 续跑次数超过配置上限被明确拒绝；
- 续跑结束后无后台残留任务、无半更新状态。

每个用例通过 logging 打印输入与判定依据；全程不访问网络。
"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from app.actions import register, call_counts
from app.errors import (
    ResumeConflictError,
    ResumeLimitExceededError,
    ResumeNotAllowedError,
    RunForbiddenError,
    RunNotFoundError,
)
from app.executor import Executor
from app.models import RunStatus, StepStatus
from app.service import WorkflowService

log = logging.getLogger("resume-test")


def _svc(**kwargs) -> WorkflowService:
    exec_kwargs = kwargs.pop("executor_kwargs", {})
    return WorkflowService(executor=Executor(**exec_kwargs), **kwargs)


def _wait_terminal(svc: WorkflowService, run_id: str, timeout: float = 10.0):
    record = svc.wait(run_id, timeout=timeout)
    with record.lock:
        status = record.status
    log.info("判定依据: run %s 终态=%s reason=%s steps=%s resume_count=%d",
             run_id[:8], status.value, record.reason,
             {k: v.status.value for k, v in record.steps.items()},
             record.resume_count)
    assert status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                      RunStatus.CANCELLED, RunStatus.TIMED_OUT), "运行未在时限内结束"
    return record


# ---- 1. 失败后续跑成功，被搁置的下游被带起 ---------------------------------

def test_resume_after_failure_succeeds_and_unblocks_downstream():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        # flaky 前 2 次失败、第 3 次成功；首轮只给 1 次重试 -> 2 次尝试后失败
        {"id": "up", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 2},
         "max_retries": 1},
        {"id": "down", "action": "echo",
         "params": {"value": "ok"}, "depends_on": ["up"]}]}
    log.info("输入: %s", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["up"].status == StepStatus.FAILED
    assert rec.steps["up"].attempts == 2
    assert rec.steps["down"].status == StepStatus.SKIPPED
    assert rec.steps["down"].attempts == 0
    log.info("首轮结束: up=FAILED(attempts=2) down=SKIPPED(被搁置，未真正执行)")

    resume_id = svc.resume("alice", rid)
    log.info("输入: 对失败运行 %s 发起续跑，resume_id=%s", rid[:8], resume_id[:8])
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s up.attempts=%d（续跑重新尝试，"
             "第 3 次 flaky 成功）down.status=%s down.attempts=%d output=%s",
             rec.status.value, rec.steps["up"].attempts,
             rec.steps["down"].status.value, rec.steps["down"].attempts,
             rec.steps["down"].output)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["up"].status == StepStatus.SUCCEEDED
    assert rec.steps["up"].attempts == 1
    assert rec.steps["down"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].attempts == 1
def test_resume_skips_succeeded_steps_and_keeps_side_effect_once():
    call_counts.clear()
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        # 幂等副作用步骤：首轮成功，续跑时不得再真正执行
        {"id": "pay", "action": "record", "params": {"key": "order-9"},
         "idempotency_key": "order-9"},
        # bad：前 2 次尝试失败、第 3 次成功；首轮 1 次重试 -> 失败
        {"id": "bad", "action": "flaky",
         "params": {"_flaky_counter": {}, "fail_times": 2},
         "max_retries": 1},
        # 首轮被搁置；续跑时 bad 补齐后执行，且只执行一次
        {"id": "late", "action": "record", "params": {"key": "order-10"},
         "depends_on": ["bad"], "idempotency_key": "order-10"}]}
    log.info("输入: %s（pay 首轮成功，bad 首轮失败，late 被搁置）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["pay"].status == StepStatus.SUCCEEDED
    assert rec.steps["bad"].status == StepStatus.FAILED
    assert rec.steps["late"].status == StepStatus.SKIPPED
    pay_output_after_first = dict(rec.steps["pay"].output)
    log.info("首轮: pay.output=%s record 实际调用次数=%d",
             pay_output_after_first, call_counts.get("record", 0))

    resume_id = svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s pay.output=%s（须与首轮一致，不重新执行）"
             "record 实际调用次数=%d（首轮只有 pay=1；续跑只新增 late=2）"
             "bad.attempts=%d late.attempts=%d",
             rec.status.value, rec.steps["pay"].output,
             call_counts.get("record", 0),
             rec.steps["bad"].attempts, rec.steps["late"].attempts)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["pay"].output == pay_output_after_first
    assert rec.steps["pay"].attempts == 1, "成功步骤沿用旧结果，attempts 不增加"
    assert rec.steps["late"].status == StepStatus.SUCCEEDED
    assert rec.steps["late"].attempts == 1
    # record 动作真实调用次数：首轮只有 pay 一次（late 被搁置），续跑只新增 late 一次
    assert call_counts.get("record") == 2, (
        f"成功步骤不得重复执行，record 调用次数={call_counts.get('record')}")
    # 账本里每个逻辑副作用只生效一次
    assert rec.effects["order-9"] == 1
    assert rec.effects["order-10"] == 1


# ---- 3. 运行超时后可以续跑并成功 ------------------------------------------

def test_resume_after_run_timeout_succeeds():
    def sleep_first_only(params):
        counter = params["_counter"]
        if counter["n"] == 0:
            counter["n"] += 1
            time.sleep(0.4)  # 首轮拖过 run_timeout 被终止
        return {"ran_after": counter["n"]}
    register("sleep_first_only", sleep_first_only)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 0.1,
                                "backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep_first_only",
         "params": {"_counter": {"n": 0}}},
        {"id": "down", "action": "echo", "params": {"value": 42},
         "depends_on": ["slow"]}]}
    log.info("输入: %s（首轮总耗时上限 0.1s，slow 睡 0.4s 必超时）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.TIMED_OUT
    assert not svc._threads[rid].is_alive(), "超时后执行线程必须已回收"
    log.info("首轮 TIMED_OUT，发起续跑：续跑重新获得完整的单次总耗时预算")

    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s slow.output=%s down.output=%s",
             rec.status.value, rec.steps["slow"].output,
             rec.steps["down"].output)
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.steps["slow"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].status == StepStatus.SUCCEEDED
    assert rec.steps["down"].output == {"echo": 42}
    assert not svc._threads[rid].is_alive()


# ---- 4. 重复续跑幂等：相同 (提交者, 幂等标识) 只生效一次 --------------------

def test_duplicate_resume_same_key_returns_same_resume_id():
    from app.errors import RetryableStepError

    def flaky_then_slow(params):
        counter = params["_counter"]
        counter["n"] += 1
        if counter["n"] <= 2:
            raise RetryableStepError("前两次失败")
        time.sleep(0.3)  # 成功的那次跑慢一点，制造确定的“续跑推进中”窗口
        return {"ok": True, "attempts": counter["n"]}
    register("flaky_then_slow", flaky_then_slow)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 10,
                                "backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        # 前 2 次尝试失败、第 3 次成功；首轮只给 1 次重试 -> 失败；续跑成功且慢
        {"id": "a", "action": "flaky_then_slow",
         "params": {"_counter": {"n": 0}}, "max_retries": 1}]}
    log.info("输入: %s，两轮续跑都带相同 idempotency_key=resume-1", raw)
    rid = svc.submit("alice", raw)
    _wait_terminal(svc, rid)

    resume1 = svc.resume("alice", rid, idempotency_key="resume-1")
    time.sleep(0.05)  # 确保第一个续跑已进入推进中
    resume2 = svc.resume("alice", rid, idempotency_key="resume-1")
    log.info("判定依据: resume1=%s resume2=%s（必须一致）", resume1[:8], resume2[:8])
    assert resume1 == resume2
    # 不同的幂等标识视为另一次续跑请求，在续跑推进中提交 -> 冲突
    with pytest.raises(ResumeConflictError) as dup:
        svc.resume("alice", rid, idempotency_key="resume-other")
    log.info("判定依据: 同运行推进中用另一 key 续跑 -> code=%s（与重复提交可区分）",
             dup.value.code)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: resume_count=%d（=1，重复请求不产生第二次续跑）"
             "status=%s", rec.resume_count, rec.status.value)
    assert rec.resume_count == 1
    assert rec.status == RunStatus.SUCCEEDED


# ---- 5. 并发续跑：恰有一个成功，其余得到 resume_conflict --------------------

def test_concurrent_resume_only_one_succeeds():
    from app.errors import RetryableStepError

    def flaky_then_slow(params):
        counter = params["_counter"]
        counter["n"] += 1
        if counter["n"] <= 2:
            raise RetryableStepError("前两次失败")
        time.sleep(0.3)  # 制造稳定的续跑执行窗口，便于并发请求撞在一起
        return {"ok": True}
    register("flaky_then_slow", flaky_then_slow)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 10,
                                "backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "a", "action": "flaky_then_slow",
         "params": {"_counter": {"n": 0}}, "max_retries": 1}]}
    rid = svc.submit("alice", raw)
    _wait_terminal(svc, rid)
    log.info("输入: 运行已 FAILED，并发发起 4 个不带 key 的续跑请求")

    results: list[str] = []
    errors: list[Exception] = []
    start = threading.Barrier(4)

    def do_resume() -> None:
        start.wait()
        try:
            results.append(svc.resume("alice", rid))
        except Exception as exc:  # noqa: BLE001 - 断言时再分类
            errors.append(exc)

    threads = [threading.Thread(target=do_resume) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rec = _wait_terminal(svc, rid)
    alive = svc._threads[rid].is_alive()
    log.info("判定依据: 成功续跑数=%d 冲突数=%d 冲突类型=%s 终态=%s 线程存活=%s",
             len(results), len(errors), [type(e).__name__ for e in errors],
             rec.status.value, alive)
    assert len(results) == 1, "并发续跑只允许一个真正生效"
    assert len(errors) == 3
    assert all(isinstance(e, ResumeConflictError) for e in errors)
    assert not alive, "续跑结束后不得残留后台线程"


# ---- 6. 续跑前置条件：未结束 / 成功 / 取消 都明确拒绝 -----------------------

def test_resume_rejected_while_running_and_after_success_and_cancel():
    svc = _svc(executor_kwargs={"run_timeout_seconds": 10})
    # 6a. 初始执行尚未结束 -> resume_not_allowed
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.4}}]})
    time.sleep(0.05)
    log.info("输入: 运行进行中发起续跑")
    with pytest.raises(ResumeNotAllowedError) as running:
        svc.resume("alice", rid)
    log.info("判定依据: 进行中续跑 code=%s（明确拒绝，区别于并发冲突）",
             running.value.code)
    _wait_terminal(svc, rid)

    # 6b. 成功终态不可续跑
    log.info("输入: 对已成功的运行发起续跑")
    with pytest.raises(ResumeNotAllowedError) as success:
        svc.resume("alice", rid)
    log.info("判定依据: 成功后续跑 code=%s message=%s",
             success.value.code, success.value)

    # 6c. 取消终态不可续跑
    rid2 = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "slow", "action": "sleep", "params": {"seconds": 0.5}}]})
    time.sleep(0.05)
    svc.cancel("alice", rid2)
    log.info("输入: 对已取消的运行发起续跑")
    with pytest.raises(ResumeNotAllowedError) as cancelled:
        svc.resume("alice", rid2)
    log.info("判定依据: 取消后续跑 code=%s（取消不可续跑）", cancelled.value.code)


def test_resume_while_resume_running_is_conflict_not_not_allowed():
    # 初始执行中 -> resume_not_allowed；续跑推进中 -> resume_conflict，两者必须可区分
    from app.errors import RetryableStepError

    def flaky_then_slow(params):
        counter = params["_counter"]
        counter["n"] += 1
        if counter["n"] <= 2:
            raise RetryableStepError("前两次失败")
        time.sleep(0.3)
        return {"ok": True}
    register("flaky_then_slow_conf", flaky_then_slow)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 10,
                                "backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "a", "action": "flaky_then_slow_conf",
         "params": {"_counter": {"n": 0}}, "max_retries": 1}]})
    _wait_terminal(svc, rid)
    svc.resume("alice", rid)
    time.sleep(0.05)
    log.info("输入: 续跑推进中再次发起续跑")
    with pytest.raises(ResumeConflictError) as conflict:
        svc.resume("alice", rid)
    log.info("判定依据: 续跑推进中 code=%s，与初始执行中的 resume_not_allowed 可区分",
             conflict.value.code)
    _wait_terminal(svc, rid)


# ---- 7. 续跑次数上限：超限明确拒绝，不会被当成成功 --------------------------

def test_resume_count_limit_enforced():
    svc = _svc(max_resumes_per_run=2,
               executor_kwargs={"backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]}
    log.info("输入: %s，max_resumes_per_run=2，共发起 3 次续跑", raw)
    rid = svc.submit("alice", raw)
    _wait_terminal(svc, rid)
    first = svc.resume("alice", rid)
    _wait_terminal(svc, rid)
    second = svc.resume("alice", rid)
    _wait_terminal(svc, rid)
    log.info("判定依据: 前两次续跑返回 %s/%s，resume_count=2", first[:8], second[:8])
    with pytest.raises(ResumeLimitExceededError) as limit:
        svc.resume("alice", rid)
    rec = svc.get_run("alice", rid)
    log.info("判定依据: 第三次续跑 code=%s message=%s，运行仍为终态=%s "
             "resume_count=%d（拒绝不得改变状态、不得当成功）",
             limit.value.code, limit.value, rec.status.value, rec.resume_count)
    assert rec.status == RunStatus.FAILED
    assert rec.resume_count == 2
    assert "上限" in str(limit.value)


# ---- 8. 越权 / 不存在：续跑的归属判定与查询一致 -----------------------------

def test_resume_forbidden_and_not_found_distinguishable():
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    rid = svc.submit("alice", {"workflow_id": "w", "steps": [
        {"id": "bad", "action": "fail", "params": {"kind": "fatal"}}]})
    _wait_terminal(svc, rid)
    log.info("输入: bob 续跑 alice 的运行；再续跑不存在的运行")
    with pytest.raises(RunForbiddenError) as forbidden:
        svc.resume("bob", rid)
    with pytest.raises(RunNotFoundError) as missing:
        svc.resume("bob", "no-such-run")
    log.info("判定依据: 越权 code=%s（403）不存在 code=%s（404），与 409 冲突可区分",
             forbidden.value.code, missing.value.code)
    assert forbidden.value.code == "run_forbidden"
    assert missing.value.code == "run_not_found"


# ---- 9. 续跑过程中取消：线程回收、无半更新状态 ------------------------------

def test_cancel_during_resume_leaves_no_background_work():
    from app.errors import RetryableStepError

    def flaky_then_very_slow(params):
        counter = params["_counter"]
        counter["n"] += 1
        if counter["n"] <= 2:
            raise RetryableStepError("前两次失败")
        time.sleep(1.0)  # 续跑成功的那一步跑得久，给取消留出窗口
        return {"done": True}
    register("flaky_then_very_slow", flaky_then_very_slow)

    svc = _svc(executor_kwargs={"run_timeout_seconds": 30,
                                "backoff_base_seconds": 0.01})
    raw = {"workflow_id": "w", "steps": [
        {"id": "a", "action": "flaky_then_very_slow",
         "params": {"_counter": {"n": 0}}, "max_retries": 1}]}
    log.info("输入: %s（首轮失败，续跑执行到一半时取消）", raw)
    rid = svc.submit("alice", raw)
    _wait_terminal(svc, rid)
    svc.resume("alice", rid)
    time.sleep(0.1)
    log.info("输入: 续跑推进中调用 cancel")
    svc.cancel("alice", rid)
    rec = svc.get_run("alice", rid)
    alive = svc._threads[rid].is_alive()
    log.info("判定依据: 取消后 status=%s reason=%s 线程存活=%s steps=%s",
             rec.status.value, rec.reason, alive,
             {k: v.status.value for k, v in rec.steps.items()})
    assert rec.status == RunStatus.CANCELLED
    assert not alive, "取消续跑后不得残留后台推进的任务"
    assert all(r.status in (StepStatus.CANCELLED, StepStatus.SUCCEEDED)
               for r in rec.steps.values()), "不得有半更新状态"
    # 被取消的续跑轮次已经计入次数；且取消终态不能再续跑
    assert rec.resume_count == 1
    with pytest.raises(ResumeNotAllowedError):
        svc.resume("alice", rid)


# ---- 10. 续跑复用既有上限：并发度上限照旧生效 --------------------------------

def test_resume_respects_concurrency_limit():
    def flaky_then_sleep(params):
        counter = params["_counter"]
        counter["n"] += 1
        if counter["n"] <= 2:
            from app.errors import RetryableStepError
            raise RetryableStepError("前两次失败")
        time.sleep(0.1)
        return {"slept": True}
    register("flaky_then_sleep", flaky_then_sleep)

    svc = _svc(executor_kwargs={"max_concurrency": 2,
                                "run_timeout_seconds": 30,
                                "backoff_base_seconds": 0.01})
    # 首轮：a 失败，4 个依赖 a 的下游全部被搁置；
    # 续跑：a 补齐成功，4 个下游按并发度=2 分两批跑（每批 0.1s）
    steps = [{"id": "a", "action": "flaky_then_sleep",
              "params": {"_counter": {"n": 0}}, "max_retries": 1}]
    steps += [{"id": f"d{i}", "action": "sleep",
               "params": {"seconds": 0.1}, "depends_on": ["a"]}
              for i in range(4)]
    log.info("输入: %d 个步骤，续跑时 4 个下游并发度上限=2", len(steps))
    rid = svc.submit("alice", {"workflow_id": "w", "steps": steps})
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    svc.resume("alice", rid)
    start = time.monotonic()
    rec = _wait_terminal(svc, rid)
    elapsed = time.monotonic() - start
    log.info("判定依据: 续跑耗时 %.3fs（4 个 0.1s 下游、并发=2，应 >=0.18s "
             "且 <0.4s）status=%s", elapsed, rec.status.value)
    assert rec.status == RunStatus.SUCCEEDED
    assert 0.18 <= elapsed < 0.4


# ---- 11. 带幂等键的步骤即使上一轮失败，续跑时副作用也不重复 ------------------

def test_resume_idempotent_failed_step_not_duplicated():
    # record_then_fail：先写账本再抛可重试错误（“效果生效但响应丢失”）。
    # 首轮不给重试 -> 步骤 FAILED，但副作用已生效一次；续跑命中持久账本，
    # 不再调用动作、不写第二次，直接采用缓存结果而成功。
    svc = _svc(executor_kwargs={"backoff_base_seconds": 0.01})
    call_counts.clear()
    raw = {"workflow_id": "w", "steps": [
        {"id": "pay", "action": "record_then_fail",
         "params": {"key": "order-77"}, "max_retries": 0,
         "idempotency_key": "order-77"}]}
    log.info("输入: %s（首轮仅 1 次尝试：副作用生效但响应丢失，步骤 FAILED）", raw)
    rid = svc.submit("alice", raw)
    rec = _wait_terminal(svc, rid)
    assert rec.status == RunStatus.FAILED
    assert rec.steps["pay"].status == StepStatus.FAILED
    assert rec.effects["order-77"] == 1, "首轮副作用只生效一次"
    calls_first = call_counts.get("record_then_fail", 0)
    log.info("首轮: 动作真实调用 %d 次，账本 order-77=%d",
             calls_first, rec.effects["order-77"])

    svc.resume("alice", rid)
    rec = _wait_terminal(svc, rid)
    log.info("判定依据: 续跑后 status=%s pay.output=%s 账本=%d 动作总调用=%d（"
             "续跑不得再调用，副作用不产生第二次）",
             rec.status.value, rec.steps["pay"].output,
             rec.effects["order-77"],
             call_counts.get("record_then_fail", 0))
    assert rec.status == RunStatus.SUCCEEDED
    assert rec.effects["order-77"] == 1
    assert call_counts.get("record_then_fail") == calls_first, (
        "续跑必须命中幂等缓存，不能再真正执行该动作")
