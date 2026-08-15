"""FakeClock：测试侧时钟（03 §10.2"FakeClock 在测试侧"；M1 册 M1.5）。

只管 worker 进程内的调度节拍（tick/心跳/T_grace）——管不了 SQL 里的 now()
（M1 册冻结区 2.5 #21）：DB 侧租约过期一律用 UPDATE 直接制造，不靠推时钟。
"""

import asyncio
from datetime import datetime, timedelta


class FakeClock:
    """now() 返回内部时刻；sleep(s) 挂起直到 advance() 累计推进越过到期点。"""

    def __init__(self, start: datetime) -> None:
        self._now = start
        self._elapsed = 0.0
        self._sleepers: list[tuple[float, asyncio.Event]] = []

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        """推进虚拟时间并唤醒到期睡眠者（sync 调用；唤醒发生在下个事件循环 tick）。"""
        self._elapsed += seconds
        self._now = self._now + timedelta(seconds=seconds)
        for due, event in list(self._sleepers):
            if due <= self._elapsed:
                event.set()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        due = self._elapsed + seconds
        event = asyncio.Event()
        entry = (due, event)
        self._sleepers.append(entry)
        try:
            await event.wait()
        finally:
            self._sleepers.remove(entry)
