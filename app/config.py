"""限制项配置：并发度、重试、超时、步骤数上限。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EngineConfig:
    max_concurrency: int = 4          # 并行执行步骤数上限
    max_steps_per_run: int = 64       # 单次运行步骤总数上限
    max_run_seconds: float = 60.0     # 单次运行总耗时上限
    default_step_timeout: float = 10.0
    max_retries: int = 3              # 可重试失败的重试次数上限
    retry_backoff_base: float = 0.05  # 退避基数（秒），指数退避
