"""M0.7 已入库冒烟带回放测试：文本非空、cost 与带内一致、零 HTTP（施工手册 M0.7）。

冒烟带未录制时本测试因 CassetteMissingError 失败——这是预期行为（缺带即失败，
绝不静默转真实调用）；录制入库后转绿。
"""

from decimal import Decimal
from typing import Any

import respx

from argus.core.cassette import CassetteStore
from argus.core.config import ArgusSettings
from argus.core.llm import CallContext, LLMClient, Message
from argus.core.types import ModelTier


def _settings(**overrides: Any) -> ArgusSettings:
    return ArgusSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


@respx.mock  # 不注册任何路由：一旦发出真实 HTTP 立刻报 unmocked（回放必须零网络）
async def test_m0_smoke_cassette_replays() -> None:
    store = CassetteStore()  # 默认根 tests/cassettes——消费已入库的 m0-smoke 带
    client = LLMClient(_settings(), cassette_store=store)
    result = await client.complete(
        tier=ModelTier.FLASH,
        messages=[Message("user", "只回答两个字：收到")],
        ctx=CallContext(scenario_id="m0-smoke", node_id="smoke", attempt=1),
        max_tokens=16,
    )
    await client.aclose()
    cassette = store.load("m0-smoke")
    assert result.text
    assert result.cost_yuan == Decimal(cassette.entries[0].cost_yuan)
    assert len(respx.calls) == 0  # 回放零 HTTP
