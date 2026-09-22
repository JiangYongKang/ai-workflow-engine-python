"""并发与取消测试：并行执行、并发取消无残留、运行级超时、并发查询一致性。"""
from __future__ import annotations

import threading
import time

from app.config import EngineConfig
from app.executor import RunExecutor
from app.models import RunStatus, StepStatus, TERMINAL_RUN_STATUSES
from app.providers import default_registry
from app.store import RunStore
from app.validation import parse_definition, validate_definition
from tests.conftest import log_case


def _submit(engine, raw, key, owner="alice"):
    config, store, executor = engine
    defn = parse_definition(raw)
    validate_definition(defn, config.max_steps_per_run)
    rec, _ = store.submit(owner, key, defn)
    executor.start(rec.run_id)
    return rec


def test_independent_steps_run_in_parallel(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": f"s{i}", "action": "slow", "params": {"duration": 0.3}}
        for i in range(4)]}
    log_case("并行执行", {"steps": 4, "each": "0.3s", "max_concurrency": 4},
             "4 个无依赖步骤在并发度 4 下应并行完成，总耗时应远小于串行的 1.2s")
    t0 = time.monotonic()
    rec = _submit(engine, raw, "k-par")
    r = engine[2].wait(rec.run_id, timeout=30)
    elapsed = time.monotonic() - t0
    print(f"  实际耗时: {elapsed:.2f}s")
    assert r.status == RunStatus.SUCCEEDED
    assert elapsed < 1.0


def test_concurrent_cancel_leaves_no_background_work(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": f"s{i}", "action": "slow", "params": {"duration": 5}}
        for i in range(3)] + [
        {"step_id": "down", "action": "echo", "depends_on": ["s0"]}]}
    log_case("并发取消", {"steps": "3 个 5s 慢步骤 + 1 个下游", "cancel": "多线程同时取消"},
             "取消后运行应为 CANCELLED/RUN_CANCELLED，在途步骤 CANCELLED，未启动步骤不得运行；"
             "wait 返回后不得有残留后台任务（done_event 已置位且全部步骤到达终态）")
    rec = _submit(engine, raw, "k-cancel")
    time.sleep(0.1)  # 让步骤进入 RUNNING
    threads = [threading.Thread(target=engine[2].cancel, args=(rec.run_id,))
               for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r = engine[2].wait(rec.run_id, timeout=30)
    assert r.status == RunStatus.CANCELLED
    assert r.error_code == "RUN_CANCELLED"
    for rec_s in r.steps.values():
        # 终态只能是：已取消 / 已完成（取消前刚好跑完）/ 因上游未成功而跳过
        assert rec_s.status in (StepStatus.CANCELLED, StepStatus.SUCCEEDED, StepStatus.SKIPPED)
    # 下游绝未执行（与"失败"和"成功"都可区分）
    assert r.steps["down"].status in (StepStatus.CANCELLED, StepStatus.SKIPPED)
    assert r.steps["down"].attempts == 0
    # done_event 已置位说明协调线程已退出，无后台残留
    ctl = engine[2]._get_control(rec.run_id)
    assert ctl.done_event.is_set()


def test_run_level_timeout(engine):
    config, store, _ = engine
    fast = EngineConfig(max_run_seconds=0.3, retry_backoff_base=0.01)
    ex = RunExecutor(fast, default_registry(), store)
    try:
        raw = {"workflow_id": "w", "steps": [
            {"step_id": "a", "action": "slow", "params": {"duration": 5}}]}
        log_case("运行级超时", {"max_run_seconds": 0.3, "step": "slow 5s"},
                 "运行总耗时超过上限应按策略终止：run=TIMED_OUT/RUN_TIMEOUT，"
                 "不得返回看似成功的状态")
        defn = parse_definition(raw)
        rec, _ = store.submit("alice", "k-run-timeout", defn)
        ex.start(rec.run_id)
        r = ex.wait(rec.run_id, timeout=30)
        assert r.status == RunStatus.TIMED_OUT
        assert r.error_code == "RUN_TIMEOUT"
        assert r.steps["a"].status != StepStatus.SUCCEEDED
    finally:
        ex.shutdown()


def test_concurrent_status_queries_consistent(engine):
    raw = {"workflow_id": "w", "steps": [
        {"step_id": "a", "action": "slow", "params": {"duration": 0.3}},
        {"step_id": "b", "action": "echo", "depends_on": ["a"]}]}
    log_case("并发查询一致性", raw["steps"],
             "执行推进的同时多线程读取状态，每次读到的 run.status 都必须是合法枚举值，"
             "且步骤状态始终是合法终态或中间态，不得出现撕裂")
    rec = _submit(engine, raw, "k-query")
    store = engine[1]
    errors = []

    def reader():
        for _ in range(50):
            r = store.get_for_owner(rec.run_id, "alice")
            if r.status not in RunStatus:
                errors.append(f"bad run status {r.status}")
            for s in r.steps.values():
                if s.status not in StepStatus:
                    errors.append(f"bad step status {s.status}")
            time.sleep(0.005)

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    r = engine[2].wait(rec.run_id, timeout=30)
    for t in readers:
        t.join()
    assert not errors
    assert r.status == RunStatus.SUCCEEDED
    assert r.status in TERMINAL_RUN_STATUSES
