"""时钟注入（03 §10.2：engine 一切时间经 Clock，测试侧 FakeClock 驱动）。"""

import asyncio
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...  # tz-aware UTC

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    """生产实现。全项目唯一允许 datetime.now(UTC) 的位置——其余裸调时间即打回。"""

    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
