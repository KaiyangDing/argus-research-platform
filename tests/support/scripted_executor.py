"""ScriptedExecutor：剧本化执行器（03 §10.2 逐条；M1 册 M1.5，EFFECTFUL 剧本 M1.9 扩展）。

剧本键 (node_id, attempt)，缺省 "success"；success 产物 payload 由 (node, attempt, seed)
确定可复算——并发回放的工件集合比对靠它（03 §10.3）。测试件，绝不进 argus/。
"""

from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel

from argus.core.types import ArtifactKind, NodeId
from argus.engine.graph import FailureClass
from argus.engine.ports import (
    ArtifactDraft,
    NodeContext,
    NodeOutcome,
    OutcomeDegraded,
    OutcomeFailure,
    OutcomeSuccess,
    ReplanSignalDraft,
)

ScriptAction = Literal[
    "success",
    "success_with_signal",
    "degraded",
    "fail_retryable",
    "fail_permanent",
    "pure_replan_signal",
    "timeout",
    "hang",
    "crash",
]


class ScriptEntry(BaseModel):
    action: ScriptAction
    artifact_kind: ArtifactKind = ArtifactKind.RESEARCH_NOTE
    hang_seconds: float = 3600.0  # hang/timeout 用（经 ctx.clock.sleep，FakeClock 可驱动）


ScriptTable = Mapping[tuple[NodeId, int], ScriptEntry]


class ScriptedExecutor:
    """按剧本表演出 NodeOutcome 全部变体；不在表内的 (node, attempt) 一律 success。"""

    def __init__(self, script: ScriptTable, *, seed: int) -> None:
        self._script = dict(script)
        self._seed = seed

    def _entry(self, ctx: NodeContext) -> ScriptEntry:
        return self._script.get((ctx.node_id, ctx.attempt), ScriptEntry(action="success"))

    def _artifact(
        self, ctx: NodeContext, entry: ScriptEntry, *, partial: bool = False
    ) -> ArtifactDraft:
        payload = {"node": str(ctx.node_id), "attempt": ctx.attempt, "seed": self._seed}
        return ArtifactDraft(
            kind=entry.artifact_kind,
            schema_name="scripted_note",
            payload=payload,
            headline=f"scripted:{entry.action}",
            partial=partial,
            content_hash=ArtifactDraft.hash_payload(payload),
        )

    async def execute(self, ctx: NodeContext) -> NodeOutcome:
        entry = self._entry(ctx)
        match entry.action:
            case "success":
                return OutcomeSuccess(artifact=self._artifact(ctx, entry))
            case "success_with_signal":
                return OutcomeSuccess(
                    artifact=self._artifact(ctx, entry),
                    replan_signal=ReplanSignalDraft(kind="coverage_gap"),
                )
            case "degraded":
                return OutcomeDegraded(artifact=self._artifact(ctx, entry, partial=True))
            case "fail_retryable":
                return OutcomeFailure(error={"cause": "scripted_retryable"}, retryable=True)
            case "fail_permanent":
                return OutcomeFailure(
                    failure_class=FailureClass.TOOL_ERROR,
                    error={"cause": "scripted_permanent"},
                    retryable=False,
                )
            case "pure_replan_signal":
                # T6a 形态：无有效产出、只带重规划信号（03 §2.2；判定表信号压倒重试）
                return OutcomeFailure(
                    error={"cause": "scripted_replan"},
                    retryable=False,
                    replan_signal=ReplanSignalDraft(kind="node_replan"),
                )
            case "timeout" | "hang":
                await ctx.clock.sleep(entry.hang_seconds)
                return OutcomeSuccess(artifact=self._artifact(ctx, entry))
            case "crash":
                raise RuntimeError("scripted crash")
        raise AssertionError(f"unknown action: {entry.action}")

    async def resume(self, ctx: NodeContext, from_step: int) -> NodeOutcome:
        """M1.5 形态：PURE 剧本 resume ≡ execute（步骤级续跑 M1.9 扩展）。"""
        return await self.execute(ctx)
