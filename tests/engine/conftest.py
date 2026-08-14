"""engine 测试专用夹具（M1 册 M1.1）。

网络封禁：M1 全部测试零真实 LLM 调用是毕业验收项本身（M1 册第 0 节第 5 行），
autouse fixture 把 httpx 的同步/异步 send 一律替换为抛错——机制保障而非君子协定。
"""

import httpx
import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """engine 测试内任何 httpx 出网即抛 RuntimeError（M1.10 有专项断言用例）。"""

    def _blocked(*args: object, **kwargs: object) -> None:
        raise RuntimeError("M1 tests must not touch network")

    monkeypatch.setattr(httpx.AsyncClient, "send", _blocked)
    monkeypatch.setattr(httpx.Client, "send", _blocked)
