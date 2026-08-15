"""M1.5 执行端口与时钟测试（M1 册测试清单，+6，纯单测）。"""

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from argus.core.types import ArtifactKind, NodeId, TaskId
from argus.engine.graph import FailureClass, TaskBrief
from argus.engine.ports import (
    ArtifactDraft,
    NodeContext,
    NullBudgetView,
    OutcomeDegraded,
    OutcomeFailure,
    OutcomeSuccess,
    ReplanSignalDraft,
)
from tests.support.fake_clock import FakeClock
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry


def _draft(**overrides: Any) -> ArtifactDraft:
    payload = {"k": "v"}
    base: dict[str, Any] = {
        "kind": ArtifactKind.RESEARCH_NOTE,
        "schema_name": "s",
        "payload": payload,
        "headline": "h",
        "content_hash": ArtifactDraft.hash_payload(payload),
    }
    base.update(overrides)
    return ArtifactDraft(**base)


def _ctx(clock: FakeClock, node_id: NodeId | None = None, attempt: int = 1) -> NodeContext:
    return NodeContext(
        task_id=TaskId(uuid.uuid4()),
        node_id=node_id if node_id is not None else NodeId(uuid.uuid4()),
        attempt=attempt,
        brief=TaskBrief(objective="o"),
        inputs=(),
        budget=NullBudgetView(),
        cancel_token=None,  # M1.12 落地后收紧为 CancelToken
        step_journal=None,  # M1.9 落地后收紧为 StepJournal | None
        clock=clock,
    )


async def test_fake_clock_sleep_wakes_on_advance() -> None:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    task = asyncio.ensure_future(clock.sleep(30))
    await asyncio.sleep(0)  # 让 sleeper 注册进时钟
    clock.advance(29)
    await asyncio.sleep(0)
    assert not task.done()  # 29 < 30：仍挂起
    clock.advance(1)
    await asyncio.wait_for(task, timeout=5)  # 真实时钟护栏：挂死即失败而非无限等


def test_artifact_draft_hash_canonical_and_shape() -> None:
    h1 = ArtifactDraft.hash_payload({"a": 1, "b": {"x": "营收"}})
    h2 = ArtifactDraft.hash_payload({"b": {"x": "营收"}, "a": 1})
    assert h1 == h2  # canonical json：键序无关（冻结区 2.5 #19）
    assert h1.startswith("sha256:")
    assert len(h1) == len("sha256:") + 64
    with pytest.raises(ValidationError):
        _draft(content_hash="md5:abc")  # 坏前缀拒收


def test_outcome_degraded_requires_partial() -> None:
    OutcomeDegraded(artifact=_draft(partial=True))  # 合法：降级产物
    with pytest.raises(ValidationError):
        OutcomeDegraded(artifact=_draft(partial=False))  # 降级必须 partial（03 §6.4）


async def test_scripted_success_and_signal_variants() -> None:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    node = NodeId(uuid.uuid4())
    ex = ScriptedExecutor(
        {
            (node, 1): ScriptEntry(action="success", artifact_kind=ArtifactKind.REPORT_FINAL),
            (node, 2): ScriptEntry(action="success_with_signal"),
            (node, 3): ScriptEntry(action="pure_replan_signal"),
        },
        seed=42,
    )
    ok = await ex.execute(_ctx(clock, node, 1))
    assert isinstance(ok, OutcomeSuccess)
    assert ok.artifact.kind is ArtifactKind.REPORT_FINAL  # 剧本可指定 sink 产 report_final
    assert ok.artifact.payload == {"node": str(node), "attempt": 1, "seed": 42}  # 确定可复算
    assert ok.replan_signal is None
    with_sig = await ex.execute(_ctx(clock, node, 2))
    assert isinstance(with_sig, OutcomeSuccess)
    assert with_sig.replan_signal is not None
    assert with_sig.replan_signal.kind == "coverage_gap"
    pure = await ex.execute(_ctx(clock, node, 3))
    assert isinstance(pure, OutcomeFailure)  # T6a：无产出、只带信号
    assert pure.replan_signal is not None
    assert pure.replan_signal.kind == "node_replan"
    default = await ex.execute(_ctx(clock))  # 不在剧本表 → 缺省 success
    assert isinstance(default, OutcomeSuccess)


async def test_scripted_failure_variants() -> None:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    node = NodeId(uuid.uuid4())
    ex = ScriptedExecutor(
        {
            (node, 1): ScriptEntry(action="fail_retryable"),
            (node, 2): ScriptEntry(action="fail_permanent"),
            (node, 3): ScriptEntry(action="crash"),
        },
        seed=7,
    )
    retryable = await ex.execute(_ctx(clock, node, 1))
    assert isinstance(retryable, OutcomeFailure)
    assert retryable.retryable is True
    assert retryable.failure_class is None  # 重试路径可 None（终态才必填）
    permanent = await ex.execute(_ctx(clock, node, 2))
    assert isinstance(permanent, OutcomeFailure)
    assert permanent.retryable is False
    assert permanent.failure_class is FailureClass.TOOL_ERROR
    with pytest.raises(RuntimeError, match="scripted crash"):
        await ex.execute(_ctx(clock, node, 3))


def test_replan_signal_default_severity() -> None:
    # 速查表 C.4：四 kind 缺省权重 2/2/1/3；显式给定优先
    assert ReplanSignalDraft(kind="node_replan").severity == 2
    assert ReplanSignalDraft(kind="evidence_conflict").severity == 2
    assert ReplanSignalDraft(kind="coverage_gap").severity == 1
    assert ReplanSignalDraft(kind="verifier_negative").severity == 3
    assert ReplanSignalDraft(kind="coverage_gap", severity=5).severity == 5
