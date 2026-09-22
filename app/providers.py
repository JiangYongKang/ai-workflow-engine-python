"""本地动作处理器注册表：模拟模型/工具调用，不访问网络。

所有处理器异常在此归一化为 WorkflowError，底层异常不透出。
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from .errors import ErrorCode, WorkflowError

Handler = Callable[[dict[str, Any], "StepContext"], Any]


class StepContext:
    """传给处理器的上下文：幂等键、取消标记等。"""

    def __init__(self, run_id: str, step_id: str, attempt: int,
                 cancel_event: threading.Event, ledger: "EffectLedger",
                 attempt_cancel: threading.Event | None = None) -> None:
        self.run_id = run_id
        self.step_id = step_id
        self.attempt = attempt
        self.cancel_event = cancel_event
        self.attempt_cancel = attempt_cancel or threading.Event()
        self.ledger = ledger

    def should_stop(self) -> bool:
        """运行被取消或本次尝试已超时。"""
        return self.cancel_event.is_set() or self.attempt_cancel.is_set()


class EffectLedger:
    """幂等效果账本：同一逻辑副作用（按 effect key）只实际生效一次。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._effects: dict[str, Any] = {}
        self.apply_counts: dict[str, int] = {}

    def apply_once(self, effect_key: str, effect: Callable[[], Any]) -> Any:
        """同一 effect_key 只执行一次 effect，后续调用返回首次结果。"""
        with self._lock:
            if effect_key in self._effects:
                return self._effects[effect_key]
        result = effect()
        with self._lock:
            if effect_key not in self._effects:
                self._effects[effect_key] = result
                self.apply_counts[effect_key] = self.apply_counts.get(effect_key, 0) + 1
            return self._effects[effect_key]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, handler: Handler) -> None:
        self._handlers[name] = handler

    def names(self) -> list[str]:
        return sorted(self._handlers)

    def invoke(self, name: str, params: dict[str, Any], ctx: StepContext) -> Any:
        """调用处理器并把一切异常归一化为 WorkflowError。"""
        handler = self._handlers.get(name)
        if handler is None:
            raise WorkflowError(ErrorCode.INVALID_DEFINITION, f"unknown action: {name!r}")
        try:
            return handler(params, ctx)
        except WorkflowError:
            raise
        except Exception as exc:  # 底层异常归一化，不原样透出
            raise WorkflowError(ErrorCode.NON_RETRYABLE_FAILURE, f"action {name!r} failed: {type(exc).__name__}") from None


def default_registry() -> HandlerRegistry:
    """内置本地处理器：echo / fail / slow / effect。"""
    reg = HandlerRegistry()

    def echo(params: dict[str, Any], ctx: StepContext) -> Any:
        return {"echo": params.get("value"), "attempt": ctx.attempt}

    def fail(params: dict[str, Any], ctx: StepContext) -> Any:
        """模拟失败：retryable 决定错误类型；fail_times 控制前 N 次尝试失败后成功。"""
        retryable = bool(params.get("retryable", False))
        fail_times = int(params.get("fail_times", 10**9))
        if ctx.attempt <= fail_times:
            code = ErrorCode.RETRYABLE_FAILURE if retryable else ErrorCode.NON_RETRYABLE_FAILURE
            raise WorkflowError(code, f"simulated {'retryable' if retryable else 'non-retryable'} failure")
        return {"recovered_on_attempt": ctx.attempt}

    def slow(params: dict[str, Any], ctx: StepContext) -> Any:
        """模拟耗时调用，响应取消。"""
        duration = float(params.get("duration", 1.0))
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            if ctx.should_stop():
                raise WorkflowError(ErrorCode.RUN_CANCELLED, "cancelled or timed out while running")
            time.sleep(0.01)
        return {"slept": duration}

    def effect(params: dict[str, Any], ctx: StepContext) -> Any:
        """模拟带副作用的调用：同一 effect key 只实际生效一次。

        fail_times 可让前 N 次尝试在生效前抛可重试失败，用于验证重试不重复生效。
        """
        if ctx.attempt <= int(params.get("fail_times", 0)):
            raise WorkflowError(ErrorCode.RETRYABLE_FAILURE, "simulated failure before effect")
        key = str(params.get("effect_key", f"{ctx.run_id}:{ctx.step_id}"))
        return ctx.ledger.apply_once(key, lambda: {"applied": key, "attempt": ctx.attempt})

    reg.register("echo", echo)
    reg.register("fail", fail)
    reg.register("slow", slow)
    reg.register("effect", effect)
    return reg
