"""测试公共设施：所有测试只使用本地内存组件，不访问网络。"""
from __future__ import annotations

import pytest

from app.config import EngineConfig
from app.executor import RunExecutor
from app.providers import default_registry
from app.store import RunStore


@pytest.fixture()
def engine():
    """快速配置的本地引擎：退避极小以加速测试。"""
    config = EngineConfig(max_concurrency=4, max_run_seconds=10.0,
                          retry_backoff_base=0.01, default_step_timeout=5.0)
    store = RunStore()
    executor = RunExecutor(config, default_registry(), store)
    yield config, store, executor
    executor.shutdown()


def log_case(title: str, inputs: dict, rationale: str) -> None:
    """按要求在单测日志中打印输入与判定依据。"""
    print(f"\n[CASE] {title}\n  输入: {inputs}\n  判定依据: {rationale}")
