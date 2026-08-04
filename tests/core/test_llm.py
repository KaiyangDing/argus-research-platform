"""M0.6 LLM 薄客户端测试（施工手册 M0.6 测试清单）。

respx 全量 mock 百炼端点（assert_all_mocked，零真实网络）；sleep 注入记录器；
fake 价目表注入，不依赖 MODEL_PRICES 真实数值。测试用 demo+live 组合走真实 HTTP
代码路径（replay 分派 M0.7 接入后另有回放测试）。
"""

from decimal import Decimal
from typing import Any

import httpx
import pytest
import respx

from argus.core.config import ArgusSettings
from argus.core.llm import (
    BudgetExceededError,
    BudgetGate,
    CallContext,
    LLMCallError,
    LLMClient,
    Message,
    ModelPrice,
)
from argus.core.types import ModelTier

_BASE = "https://dashscope.aliyuncs.com/compatible-mode/v1"

_PRICES = {
    "qwen-flash": ModelPrice(input_per_1k=Decimal(1), output_per_1k=Decimal(2)),
    "qwen-plus": ModelPrice(input_per_1k=Decimal(4), output_per_1k=Decimal(8)),
    "qwen-max": ModelPrice(input_per_1k=Decimal(20), output_per_1k=Decimal(40)),
    "text-embedding-v4": ModelPrice(input_per_1k=Decimal(1), output_per_1k=Decimal(0)),
}

_OK_JSON = {
    "id": "req-1",
    "model": "qwen-flash",
    "choices": [{"message": {"role": "assistant", "content": "收到"}}],
    "usage": {"prompt_tokens": 100, "completion_tokens": 50},
}


def _settings(**overrides: Any) -> ArgusSettings:
    base: dict[str, Any] = {"env": "demo", "llm_mode": "live", "bailian_api_key": "sk-test"}
    base.update(overrides)
    return ArgusSettings(_env_file=None, **base)  # type: ignore[call-arg]


def _ctx() -> CallContext:
    return CallContext(scenario_id="t", node_id="n", attempt=1)


class _SleepRecorder:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def test_tier_to_model_mapping() -> None:
    client = LLMClient(_settings(), prices=_PRICES)
    assert client.model_for(ModelTier.FLASH) == "qwen-flash"
    assert client.model_for(ModelTier.PLUS) == "qwen-plus"
    assert client.model_for(ModelTier.MAX) == "qwen-max"


@respx.mock
async def test_complete_parses_usage_and_cost() -> None:
    route = respx.post(f"{_BASE}/chat/completions").mock(
        return_value=httpx.Response(200, json=_OK_JSON)
    )
    client = LLMClient(_settings(), prices=_PRICES)
    result = await client.complete(
        tier=ModelTier.FLASH, messages=[Message("user", "你好")], ctx=_ctx()
    )
    assert route.called
    assert result.usage.input_tokens == 100
    assert result.usage.output_tokens == 50
    assert result.cost_yuan == Decimal("0.2")  # 100/1000×1 + 50/1000×2（fake 价目精确核算）
    assert result.request_id == "req-1"
    assert result.usage.estimated is False


async def test_budget_gate_blocks_before_call() -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.post(f"{_BASE}/chat/completions").mock(
            return_value=httpx.Response(200, json=_OK_JSON)
        )
        gate = BudgetGate("tiny", Decimal("0.0001"))
        client = LLMClient(_settings(), gates=[gate], prices=_PRICES)
        with pytest.raises(BudgetExceededError):
            await client.complete(
                tier=ModelTier.FLASH, messages=[Message("user", "你好")], ctx=_ctx()
            )
        assert route.call_count == 0  # 超限根本不发请求
        assert gate.spent_yuan == Decimal(0)


@respx.mock
async def test_budget_gate_settles_actual_cost() -> None:
    respx.post(f"{_BASE}/chat/completions").mock(return_value=httpx.Response(200, json=_OK_JSON))
    gate = BudgetGate("task", Decimal(100))
    client = LLMClient(_settings(), gates=[gate], prices=_PRICES)
    result = await client.complete(
        tier=ModelTier.FLASH, messages=[Message("user", "你好")], ctx=_ctx()
    )
    assert gate.spent_yuan == result.cost_yuan


@respx.mock
async def test_429_backoff_then_success() -> None:
    route = respx.post(f"{_BASE}/chat/completions")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(429),
        httpx.Response(200, json=_OK_JSON),
    ]
    sleeper = _SleepRecorder()
    client = LLMClient(_settings(), prices=_PRICES, sleep=sleeper)
    result = await client.complete(
        tier=ModelTier.FLASH, messages=[Message("user", "你好")], ctx=_ctx()
    )
    assert result.text == "收到"
    assert sleeper.calls == [0.5, 1]  # 首发不等待，两次 429 各退避一档


@respx.mock
async def test_429_exhausted_raises_congestion() -> None:
    route = respx.post(f"{_BASE}/chat/completions")
    route.side_effect = [httpx.Response(429)] * 6
    sleeper = _SleepRecorder()
    client = LLMClient(_settings(), prices=_PRICES, sleep=sleeper)
    with pytest.raises(LLMCallError) as ei:
        await client.complete(tier=ModelTier.FLASH, messages=[Message("user", "你好")], ctx=_ctx())
    assert ei.value.congestion is True
    assert sleeper.calls == [0.5, 1, 2, 4, 8]  # 六发五等，序列冻结


@respx.mock
async def test_stream_aclose_estimates_and_settles() -> None:
    sse = (
        'data: {"id":"req-s","choices":[{"delta":{"content":"你"}}]}\n\n'
        'data: {"choices":[{"delta":{"content":"好"}}]}\n\n'
        'data: {"choices":[{"delta":{"content":"世界"}}]}\n\n'
    ).encode()
    respx.post(f"{_BASE}/chat/completions").mock(
        return_value=httpx.Response(200, content=sse, headers={"content-type": "text/event-stream"})
    )
    gate = BudgetGate("stream", Decimal(100))
    client = LLMClient(_settings(), gates=[gate], prices=_PRICES)
    stream = await client.stream(tier=ModelTier.FLASH, messages=[Message("user", "问")], ctx=_ctx())
    it = stream.__aiter__()
    assert await anext(it) == "你"
    assert await anext(it) == "好"
    await stream.aclose()  # 只消费两段就中断
    assert stream.result.usage.estimated is True  # 拿不到供应商 usage → 本地估计
    assert stream.result.text == "你好"
    assert gate.spent_yuan > Decimal(0)  # 中断也要入账
