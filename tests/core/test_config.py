"""M0.4 配置收口测试（施工手册 M0.4 测试清单）。

用例经 _make() 直构（_env_file=None，不吃开发机 .env）；
涉及环境变量的用例先清空 ARGUS_* 再注入，保证本地与 CI 同结果。

mypy 注：pydantic v2 经 dataclass_transform 按字段合成 __init__ 签名，
pydantic-settings 的 _env_file 运行时入参不在其中；type: ignore 集中在 _make() 一处。
"""

import os
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from argus.core.config import ArgusSettings


def _make(**overrides: Any) -> ArgusSettings:
    return ArgusSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


def _clear_argus_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("ARGUS_"):
            monkeypatch.delenv(key)


def test_defaults_and_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_argus_env(monkeypatch)
    s = _make()
    assert s.env == "dev"
    assert s.llm_mode == "replay"
    assert s.worker_concurrency == 8
    assert s.budget_default_yuan == Decimal("3.0")
    assert s.model_fast == "qwen-flash"
    assert s.model_heavy == "qwen-plus"
    assert s.model_escalate == "qwen-max"
    assert s.embed_model == "text-embedding-v4"
    assert s.corpus_hash is None
    assert s.mcp_endpoints == {}

    monkeypatch.setenv("ARGUS_WORKER_CONCURRENCY", "4")
    assert _make().worker_concurrency == 4


def test_secret_not_leaked_in_repr() -> None:
    s = _make(bailian_api_key="sk-secret123")
    assert "sk-secret123" not in repr(s)
    assert "sk-secret123" not in str(s)
    assert str(s.bailian_api_key) == "**********"


def test_dev_forces_replay() -> None:
    with pytest.raises(ValidationError, match="replay"):
        _make(env="dev", llm_mode="record", bailian_api_key="sk-x")


def test_record_requires_api_key() -> None:
    with pytest.raises(ValidationError, match="ARGUS_BAILIAN_API_KEY"):
        _make(env="demo", llm_mode="record", bailian_api_key="")
    ok = _make(env="demo", llm_mode="record", bailian_api_key="sk-x")
    assert ok.llm_mode == "record"


def test_eval_requires_corpus_hash_and_budget() -> None:
    with pytest.raises(ValidationError, match="corpus_hash"):
        _make(env="eval", corpus_hash=None)
    with pytest.raises(ValidationError, match="corpus_hash"):
        _make(env="eval", corpus_hash="sha256:ab", budget_default_yuan=0)
    ok = _make(env="eval", corpus_hash="sha256:ab")
    assert ok.corpus_hash == "sha256:ab"


def test_mcp_endpoints_json_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_argus_env(monkeypatch)
    monkeypatch.setenv("ARGUS_MCP_ENDPOINTS", '{"kb-search":"http://mcp-kb-search:7801"}')
    s = _make()
    assert s.mcp_endpoints == {"kb-search": "http://mcp-kb-search:7801"}
