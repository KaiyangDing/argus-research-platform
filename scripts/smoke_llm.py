"""M0.7 一次性冒烟脚本（[AI写]）：record 一盘带子并立即 replay 验证。

用法（需用户明确同意后执行，录制会话预算 ≤¥5，实际预期 <¥0.1）：
    $env:ARGUS_ENV="demo"; $env:ARGUS_LLM_MODE="record"
    uv run python -m scripts.smoke_llm
    $env:ARGUS_ENV="dev"; $env:ARGUS_LLM_MODE="replay"

产物：tests/cassettes/m0-smoke/cassette.json（入库）。
"""

import asyncio
from decimal import Decimal

from argus.core.cassette import CassetteStore
from argus.core.config import ArgusSettings
from argus.core.llm import BudgetGate, CallContext, LLMClient, Message
from argus.core.types import ModelTier

_MESSAGES = [Message("user", "只回答两个字：收到")]
_MAX_TOKENS = 16


def _ctx() -> CallContext:
    return CallContext(scenario_id="m0-smoke", node_id="smoke", attempt=1)


async def main() -> None:
    settings = ArgusSettings()  # 读 .env + 进程环境（需 ARGUS_ENV=demo ARGUS_LLM_MODE=record）
    if settings.llm_mode != "record":
        raise SystemExit("请以 ARGUS_ENV=demo、ARGUS_LLM_MODE=record 运行本脚本（07 §8）")

    record_client = LLMClient(
        settings,
        gates=[BudgetGate("m0-smoke", Decimal("0.5"))],
        cassette_store=CassetteStore(),
    )
    result = await record_client.complete(
        tier=ModelTier.FLASH, messages=_MESSAGES, ctx=_ctx(), max_tokens=_MAX_TOKENS
    )
    await record_client.aclose()  # finalize：落盘 tests/cassettes/m0-smoke/cassette.json
    print(f"录制完成：text={result.text!r} cost=¥{result.cost_yuan}")

    replay_client = LLMClient(
        settings.model_copy(update={"llm_mode": "replay"}),
        cassette_store=CassetteStore(),
    )
    replayed = await replay_client.complete(
        tier=ModelTier.FLASH, messages=_MESSAGES, ctx=_ctx(), max_tokens=_MAX_TOKENS
    )
    await replay_client.aclose()
    if replayed.text != result.text:
        raise SystemExit(f"REPLAY MISMATCH: {replayed.text!r} != {result.text!r}")
    print("REPLAY OK")


if __name__ == "__main__":
    asyncio.run(main())
