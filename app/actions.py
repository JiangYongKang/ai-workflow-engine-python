"""本地动作注册表。

所有“模型/工具调用”均为本地确定性实现，不访问网络、不需要凭据。
动作抛出的任何底层异常都在 executor 中被归一化为
RetryableStepError / FatalStepError / StepTimeoutError，绝不原样透出。
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from app.errors import FatalStepError, RetryableStepError

ActionFn = Callable[[dict[str, Any]], Any]

_REGISTRY: dict[str, ActionFn] = {}


def register(name: str, fn: ActionFn) -> None:
    _REGISTRY[name] = fn


def get_action(name: str) -> ActionFn:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise FatalStepError(f"未知动作: {name!r}") from None


def _action_echo(params: dict[str, Any]) -> Any:
    return {"echo": params.get("value")}


def _action_fail(params: dict[str, Any]) -> Any:
    kind = params.get("kind", "fatal")
    if kind == "retryable":
        raise RetryableStepError(params.get("message", "可重试失败"))
    raise FatalStepError(params.get("message", "不可重试失败"))


def _action_flaky(params: dict[str, Any]) -> Any:
    """前 fail_times 次抛可重试错误，之后成功。用于验证重试与幂等。"""
    counter: dict[str, int] = params["_flaky_counter"]
    counter["n"] = counter.get("n", 0) + 1
    if counter["n"] <= int(params.get("fail_times", 0)):
        raise RetryableStepError(f"第 {counter['n']} 次尝试失败（可重试）")
    return {"attempts": counter["n"]}


def _action_sleep(params: dict[str, Any]) -> Any:
    time.sleep(float(params.get("seconds", 0.01)))
    return {"slept": params.get("seconds", 0.01)}


def _action_record(params: dict[str, Any]) -> Any:
    """模拟一次逻辑副作用：写入调用方提供的共享账本。"""
    ledger: dict[str, int] = params["_effect_ledger"]
    key = params["key"]
    ledger[key] = ledger.get(key, 0) + 1
    return {"recorded": key, "count": ledger[key]}


register("echo", _action_echo)
register("fail", _action_fail)
register("flaky", _action_flaky)
register("sleep", _action_sleep)
register("record", _action_record)

# 供测试统计动作真实调用次数（验证幂等副作用只生效一次）
call_counts: dict[str, int] = {}
_call_lock = threading.Lock()


def counted_call(name: str, fn: ActionFn, params: dict[str, Any]) -> Any:
    with _call_lock:
        call_counts[name] = call_counts.get(name, 0) + 1
    return fn(params)


def _action_record_then_fail(params: dict[str, Any]) -> Any:
    """先产生副作用再抛可重试错误：模拟“效果已生效但调用方收到失败”。

    配合 executor 的幂等账本，重试时必须命中缓存而不再调用本动作，
    从而保证同一逻辑副作用只生效一次。
    """
    ledger: dict[str, int] = params["_effect_ledger"]
    key = params["key"]
    ledger[key] = ledger.get(key, 0) + 1
    raise RetryableStepError("副作用已生效但响应丢失（可重试）")


register("record_then_fail", _action_record_then_fail)
