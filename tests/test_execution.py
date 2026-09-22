"""执行语义测试：失败重试、错误分类、下游跳过、副作用幂等、步骤超时。"""
from __future__ import annotations

from app.errors import ErrorCode
from app.models import RunStatus, StepStatus
from app.validation import parse_definition, validate_definition
from tests.conftest import log_case


def _run(engine, raw, key, owner="alice"):
    config, store, executor = engine
    defn = parse_definition(raw)
    validate_definition(defn, config.max_steps_per_run)
    rec, _ = store.submit(owner, key, defn)
    executor.start(rec.run_id)
    return executor.wait(rec.run_id, timeout=30)


def test_retryable_failure_recovers(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "fail",
         "params": {"retryable": True, "fail_times": 2}}]}
    log_case("可重试失败后恢复", raw["steps"],
             "fail_times=2 表示前 2 次尝试抛 RETRYABLE_FAILURE，第 3 次应成功；"
             "判定：步骤 SUCCEEDED 且 attempts==3")
    r = _run(engine, raw, "k-retry-ok")
    assert r.status == RunStatus.SUCCEEDED
    assert r.steps["a"].status == StepStatus.SUCCEEDED
    assert r.steps["a"].attempts == 3


def test_retries_exhausted_and_downstream_skipped(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "fail",
         "params": {"retryable": True}, "max_retries": 1},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]},
    ]}
    log_case("重试耗尽+下游跳过", raw["steps"],
             "max_retries=1 允许 2 次尝试，持续可重试失败应耗尽为 FAILED/RETRIES_EXHAUSTED；"
             "下游 b 不得执行也不得标记成功，应为 SKIPPED/UPSTREAM_FAILED")
    r = _run(engine, raw, "k-retry-exhaust")
    assert r.status == RunStatus.FAILED
    assert r.steps["a"].status == StepStatus.FAILED
    assert r.steps["a"].error_code == ErrorCode.RETRIES_EXHAUSTED.value
    assert r.steps["a"].attempts == 2
    assert r.steps["b"].status == StepStatus.SKIPPED
    assert r.steps["b"].error_code == ErrorCode.UPSTREAM_FAILED.value
    assert r.steps["b"].attempts == 0


def test_non_retryable_failure_not_retried(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "fail", "params": {"retryable": False}}]}
    log_case("不可重试失败", raw["steps"],
             "NON_RETRYABLE_FAILURE 不应触发重试；判定：attempts==1 且状态 FAILED")
    r = _run(engine, raw, "k-nonretry")
    assert r.steps["a"].status == StepStatus.FAILED
    assert r.steps["a"].error_code == ErrorCode.NON_RETRYABLE_FAILURE.value
    assert r.steps["a"].attempts == 1


def test_step_timeout_distinguished(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "slow",
         "params": {"duration": 5}, "timeout_seconds": 0.2}]}
    log_case("步骤超时", raw["steps"],
             "步骤耗时 5s 超过 timeout 0.2s，应为 TIMED_OUT/STEP_TIMEOUT，与 FAILED 可区分")
    r = _run(engine, raw, "k-step-timeout")
    assert r.steps["a"].status == StepStatus.TIMED_OUT
    assert r.steps["a"].error_code == ErrorCode.STEP_TIMEOUT.value
    assert r.status == RunStatus.FAILED


def test_side_effect_applied_once(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "effect",
         "params": {"effect_key": "ledger-1", "fail_times": 1}},
    ]}
    log_case("副作用幂等", raw["steps"],
             "第 1 次尝试在生效前失败并重试，第 2 次成功；同一 effect_key 在重试间只应实际生效一次；"
             "判定：attempts==2、输出中 effect 记录的是首次生效尝试、账本 apply_counts==1")
    config, store, executor = engine
    r = _run(engine, raw, "k-effect")
    assert r.status == RunStatus.SUCCEEDED
    assert r.steps["a"].attempts == 2
    ctl = executor._get_control(r.run_id)
    assert ctl.ledger.apply_counts["ledger-1"] == 1


def test_unknown_action_normalized(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "does-not-exist"}]}
    log_case("未知动作", raw["steps"],
             "未注册动作应归一化为 INVALID_DEFINITION 错误，不抛出底层异常")
    r = _run(engine, raw, "k-unknown")
    assert r.steps["a"].status == StepStatus.FAILED
    assert r.steps["a"].error_code == ErrorCode.INVALID_DEFINITION.value
