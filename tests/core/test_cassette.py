"""M0.7 cassette 基建测试（施工手册 M0.7 测试清单）。纯单测：内存构造 + tmp_path 存取。"""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from argus.core.cassette import (
    Cassette,
    CassetteEntry,
    CassetteHeader,
    CassetteKey,
    CassetteMismatchError,
    CassetteMissingError,
    CassettePlayer,
    CassetteRecorder,
    CassetteStore,
    scrub,
)
from argus.core.config import ArgusSettings
from argus.core.llm import BudgetExceededError, CallContext, LLMClient, Message, request_digest
from argus.core.types import ModelTier


def _settings(**overrides: Any) -> ArgusSettings:
    return ArgusSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


def _header(scenario_id: str) -> CassetteHeader:
    return CassetteHeader(
        scenario_id=scenario_id,
        corpus_hash=None,
        models=["qwen-flash"],
        recorded_at="2026-08-04T00:00:00+00:00",
        spent_yuan="0.001",
    )


def _entry(
    node_id: str, *, attempt: int = 1, call_index: int = 0, digest: str = "d", text: str = "ok"
) -> CassetteEntry:
    return CassetteEntry(
        key=CassetteKey(node_id=node_id, attempt=attempt, call_index=call_index),
        endpoint="chat",
        request_digest=digest,
        response_text=text,
        model="qwen-flash",
        usage={"input": 1, "output": 1, "cached": 0},
        cost_yuan="0.001",
    )


def test_replay_returns_recorded_entry(tmp_path: Path) -> None:
    store = CassetteStore(root=tmp_path)
    store.save(Cassette(header=_header("s1"), entries=[_entry("n1", digest="dg")]))
    player = CassettePlayer(store.load("s1"))
    entry = player.replay(CassetteKey(node_id="n1", attempt=1, call_index=0), "dg")
    assert entry.response_text == "ok"
    assert entry.model == "qwen-flash"


def test_missing_scenario_raises_not_silent(tmp_path: Path) -> None:
    store = CassetteStore(root=tmp_path)
    with pytest.raises(CassetteMissingError):  # 绝不静默转真实调用
        store.load("no-such-scenario")


def test_missing_key_raises(tmp_path: Path) -> None:
    store = CassetteStore(root=tmp_path)
    store.save(Cassette(header=_header("s2"), entries=[_entry("n1", call_index=0)]))
    player = CassettePlayer(store.load("s2"))
    with pytest.raises(CassetteMissingError):
        player.replay(CassetteKey(node_id="n1", attempt=1, call_index=1), "d")


def test_digest_mismatch_raises(tmp_path: Path) -> None:
    store = CassetteStore(root=tmp_path)
    store.save(Cassette(header=_header("s3"), entries=[_entry("n1", digest="old")]))
    player = CassettePlayer(store.load("s3"))
    with pytest.raises(CassetteMismatchError):  # 禁止"旧带子配新词"
        player.replay(CassetteKey(node_id="n1", attempt=1, call_index=0), "new")


def test_scrub_removes_secrets() -> None:
    dirty = {
        "Authorization": "Bearer sk-abc123456789",
        "nested": {"api_key": "plain-value", "note": "含 sk-zzzz99999999 的文本"},
        "safe": "普通内容",
    }
    cleaned = json.dumps(scrub(dirty), ensure_ascii=False)
    assert "sk-abc" not in cleaned
    assert "sk-zzzz" not in cleaned
    assert "plain-value" not in cleaned
    assert "普通内容" in cleaned


def test_recorder_session_cap_aborts(tmp_path: Path) -> None:
    recorder = CassetteRecorder(
        CassetteStore(root=tmp_path),
        "cap-test",
        corpus_hash=None,
        session_cap_yuan=Decimal("0.01"),
    )
    recorder.record(
        CassetteKey(node_id="n", attempt=1, call_index=0),
        endpoint="chat",
        request_digest="d0",
        model="qwen-flash",
        usage={"input": 1, "output": 1, "cached": 0},
        cost_yuan=Decimal("0.008"),
        response_text="a",
    )
    with pytest.raises(BudgetExceededError):  # 第二笔 0.016 > 0.01：超限中止会话
        recorder.record(
            CassetteKey(node_id="n", attempt=1, call_index=1),
            endpoint="chat",
            request_digest="d1",
            model="qwen-flash",
            usage={"input": 1, "output": 1, "cached": 0},
            cost_yuan=Decimal("0.008"),
            response_text="b",
        )


async def test_call_index_is_per_node_scope(tmp_path: Path) -> None:
    body = {
        "model": "qwen-flash",
        "messages": [{"role": "user", "content": "x"}],
        "max_tokens": 16,
    }
    digest = request_digest(body)
    entries = [
        _entry(node, call_index=idx, digest=digest, text=f"{node}{idx}")
        for node in ("a", "b")
        for idx in (0, 1)
    ]
    store = CassetteStore(root=tmp_path)
    store.save(Cassette(header=_header("scope-test"), entries=entries))
    client = LLMClient(_settings(), cassette_store=store)
    texts = []
    for node in ("a", "a", "b", "b"):
        result = await client.complete(
            tier=ModelTier.FLASH,
            messages=[Message("user", "x")],
            ctx=CallContext(scenario_id="scope-test", node_id=node, attempt=1),
            max_tokens=16,
        )
        texts.append(result.text)
    await client.aclose()
    assert texts == ["a0", "a1", "b0", "b1"]  # 各 node 从 0 起，不是全局 0,1,2,3
