"""合成图生成器与对抗执行台（03 §10.1/§10.2/§10.3；M1 册 M1.13，[AI写]）。

13a：参数化生成器 + DAG 文本渲染（本份）；13b：SimRig 注入执行台；13c：不变量断言器。

确定性口径：一切结构决策（分层/连边/纯度/剧本）吃 GraphParams.seed；节点 id 每次
generate 新生成（PK 不可复用），跨次比对按**生成序索引**对齐。
surgery 注入是 M2 接入位（冻结区 2.5 #17），M1 恒缺席。
"""

import asyncio
import random
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from typing import NamedTuple

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from argus.core.db import build_sessionmaker
from argus.core.types import ArtifactKind, ContractId, NodeId, RoleName, TaskId
from argus.engine.cancel import request_cancel, subtree_ids
from argus.engine.graph import (
    BudgetRequest,
    NodeStatus,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from argus.engine.ports import (
    ArtifactDraft,
    BudgetHint,
    ContractPair,
    NodeContext,
    NodeExecutor,
    NodeOutcome,
    NullBudgetHooks,
)
from argus.engine.store import load_graph, persist_graph
from argus.engine.validate import validate_graph
from argus.engine.worker import EngineTuning, Worker
from tests.support.fake_clock import FakeClock
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry

Script = dict[tuple[NodeId, int], ScriptEntry]

_CONTRACT = ContractId("research_memo@1")  # 全图统一契约：同 src 单契约约束平凡满足

_HANG_FOREVER = 1e9  # 超时剧本：虚拟时钟上永不醒——靠 SimRig 压缩的真实 node_timeout 判死


class GraphParams(BaseModel):
    """03 §10.1 生成器参数表逐行 + 慢节点参数（M1.14 演练注入窗口需要，实现细则）。

    fanout 解释（实现细则）：上限约束"额外父边"的密度——宽图靠 depth 压扁 + 节点数
    堆宽实现，deep 图靠 depth 拉高；fanout 数值本身不做逐节点出度硬约束。
    """

    model_config = ConfigDict(frozen=True)

    n_nodes: int  # 扫描点：20 / 100
    depth: tuple[int, int] = (3, 10)
    fanout: tuple[int, int] = (2, 20)
    effectful_ratio: float = 0.1  # PURE:EFFECTFUL 9:1 ～ 1:1
    fail_rate: float = 0.0  # 命中节点抽剧本：flaky（第 2 次成功）/永败/超时
    cancel_injections: int = 0  # 随机时刻取消随机子树（13b 消费）
    crash_injections: int = 0  # 随机时刻硬杀 worker 协程（13b 消费）
    cancel_crash_combo: int = 0  # 取消意图落库后、终态前硬杀持有 worker（13b 消费）
    slow_ratio: float = 0.0  # 成功节点混入慢剧本，保证注入时点覆盖执行中段（M1.14）
    slow_seconds: tuple[float, float] = (5.0, 15.0)
    seed: int = 0
    # surgery_injections: M2 接入位（冻结区 2.5 #17），M1 恒 0


def generate(params: GraphParams) -> tuple[PlanGraph, Script]:
    """确定性生成合法 DAG（单根/单 sink/≥1 菱形汇合）+ 配套剧本表。

    结构保证：
    - L0 单根（planner）、末层单 sink（synthesizer，两个 attempt 都产 report_final）；
    - 每个非根节点至少一个父来自紧邻上层（深度不塌）；零出度节点统一汇入 sink；
    - 至少一个非 sink 节点入度 ≥2（菱形汇合，测促升"最后一个前驱"语义）。
    """
    if params.n_nodes < 3:
        raise ValueError("n_nodes >= 3（根 + 中间 + sink）")
    rng = random.Random(params.seed)
    depth = max(3, min(rng.randint(*params.depth), params.n_nodes))

    # 分层：根层与 sink 层各 1 个，剩余节点随机塞进中间层
    counts = [1] * depth
    for _ in range(params.n_nodes - depth):
        counts[1 + rng.randrange(depth - 2)] += 1

    layers: list[list[PlanNode]] = []
    nodes: list[PlanNode] = []
    for li, cnt in enumerate(counts):
        layer: list[PlanNode] = []
        for _ in range(cnt):
            if li == 0:
                role = RoleName.PLANNER
            elif li == depth - 1:
                role = RoleName.SYNTHESIZER
            else:
                role = RoleName.RESEARCHER
            purity = Purity.EFFECTFUL if rng.random() < params.effectful_ratio else Purity.PURE
            node = PlanNode(
                id=NodeId(uuid.uuid4()),
                role=role,
                brief=TaskBrief(objective=f"synthetic-{len(nodes)}"),
                inputs=(),
                purity=purity,
                budget=BudgetRequest(tokens=10, yuan=Decimal("0.1")),
            )
            layer.append(node)
            nodes.append(node)
        layers.append(layer)
    sink = layers[-1][0]

    edges: list[PlanEdge] = []
    edge_keys: set[tuple[NodeId, NodeId]] = set()
    out_degree: dict[NodeId, int] = {n.id: 0 for n in nodes}
    in_degree: dict[NodeId, int] = {n.id: 0 for n in nodes}

    def _add_edge(src: PlanNode, dst: PlanNode) -> None:
        if src.id == dst.id or (src.id, dst.id) in edge_keys:
            return
        edge_keys.add((src.id, dst.id))
        edges.append(PlanEdge(src=src.id, dst=dst.id, contract=_CONTRACT))
        out_degree[src.id] += 1
        in_degree[dst.id] += 1

    extra_parent_cap = max(1, min(params.fanout[1], 3))  # 额外父边密度上限（实现细则）
    for li in range(1, depth):
        earlier = [n for layer in layers[:li] for n in layer]
        for node in layers[li]:
            _add_edge(rng.choice(layers[li - 1]), node)  # 至少一父来自紧邻上层
            for _ in range(rng.randint(0, extra_parent_cap - 1)):
                _add_edge(rng.choice(earlier), node)

    for node in nodes:  # 零出度统一汇入 sink：单 sink、全图工作流向终点
        if node is not sink and out_degree[node.id] == 0:
            _add_edge(node, sink)

    # 菱形保证：不存在非 sink 的入度 ≥2 节点时，强制给 L2 首节点补第二个父
    if not any(in_degree[n.id] >= 2 for n in nodes if n is not sink):
        target = layers[2][0]
        for candidate in (*layers[1], *layers[0]):
            before = in_degree[target.id]
            _add_edge(candidate, target)
            if in_degree[target.id] > before and in_degree[target.id] >= 2:
                break

    # 剧本表：sink 双 attempt 锚定 report_final；其余按 fail/slow/effectful 优先级抽取
    script: Script = {}
    for node in nodes:
        if node is sink:
            for attempt in (1, 2):  # 崩溃重领后 attempt=2 也必须产 report_final
                script[(node.id, attempt)] = ScriptEntry(
                    action="success", artifact_kind=ArtifactKind.REPORT_FINAL
                )
            continue
        roll = rng.random()
        slow_hit = rng.random() < params.slow_ratio
        slow_secs = rng.uniform(*params.slow_seconds)
        if roll < params.fail_rate:
            flavor = rng.random()
            if flavor < 0.5:  # flaky：第 1 次败、第 2 次缺省 success
                script[(node.id, 1)] = ScriptEntry(action="fail_retryable")
            elif flavor < 0.85:  # 永败：两轮皆败 → 判定表走 retry_exhausted FAILED
                script[(node.id, 1)] = ScriptEntry(action="fail_retryable")
                script[(node.id, 2)] = ScriptEntry(action="fail_retryable")
            else:  # 超时：虚拟时钟上永不醒，靠 SimRig 压缩的真实 node_timeout 判死
                script[(node.id, 1)] = ScriptEntry(action="hang", hang_seconds=_HANG_FOREVER)
                script[(node.id, 2)] = ScriptEntry(action="hang", hang_seconds=_HANG_FOREVER)
        elif node.purity is Purity.EFFECTFUL:
            steps = rng.randint(2, 4)
            step_secs = slow_secs / steps if slow_hit else 0.0  # 慢与步骤剧本可组合
            for attempt in (1, 2):  # 同步骤结构：崩溃后 attempt 2 走 resume 续跑
                script[(node.id, attempt)] = ScriptEntry(
                    action="effectful_steps", steps=steps, step_seconds=step_secs
                )
        elif slow_hit:
            script[(node.id, 1)] = ScriptEntry(action="hang", hang_seconds=slow_secs)
        # 其余缺省 success（ScriptedExecutor 的默认剧本）

    graph = PlanGraph(
        task_id=TaskId(uuid.uuid4()), version=0, nodes=tuple(nodes), edges=tuple(edges)
    )
    return graph, script


def render_ascii(graph: PlanGraph, statuses: Mapping[NodeId, str]) -> str:
    """极简 DAG 文本渲染（05 §4.5"看不见的里程碑"对策；失败用例一键复现的现场照）。

    每节点一行：层号 / 生成序号 / id 前 4 位 / 角色缩写 / 纯度缩写 / 状态 / 父序号表。
    """
    index_of = {n.id: i for i, n in enumerate(graph.nodes)}
    parents: dict[NodeId, list[int]] = {n.id: [] for n in graph.nodes}
    for e in graph.edges:
        parents[e.dst].append(index_of[e.src])
    level: dict[NodeId, int] = {}
    remaining = {n.id for n in graph.nodes}
    while remaining:  # 最长路径分层（图保证无环，循环必然收敛）
        progressed = False
        for nid in sorted(remaining, key=lambda x: index_of[x]):
            if all(graph.nodes[p].id in level for p in parents[nid]):
                level[nid] = max((level[graph.nodes[p].id] for p in parents[nid]), default=-1) + 1
                remaining.discard(nid)
                progressed = True
        if not progressed:
            raise AssertionError("render_ascii: graph has a cycle")
    lines = [f"synthetic graph: {len(graph.nodes)} nodes / {len(graph.edges)} edges"]
    for i, node in enumerate(graph.nodes):
        par = ",".join(f"#{p}" for p in sorted(parents[node.id])) or "-"
        lines.append(
            f"L{level[node.id]:<2} #{i:<3} {str(node.id)[:4]} "
            f"{node.role.value[:5]:<5} {node.purity.value[:3].upper()} "
            f"{statuses.get(node.id, '·'):<12} <- {par}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 13b · SimRig：进程内对抗执行台（真 PG + 多 worker 协程 + 共享 FakeClock）
# ---------------------------------------------------------------------------


class ExecutionRecord(NamedTuple):
    """一次**完整**执行（execute/resume 正常返回；抛异常/被取消不记）——I7 零双执行底账。"""

    kind: str  # "execute" | "resume"
    node_id: NodeId
    attempt: int
    outcome: str  # NodeOutcome 变体类名


class SimResult(NamedTuple):
    task_id: TaskId
    graph: PlanGraph
    script: Script
    statuses: dict[NodeId, str]
    attempts: dict[NodeId, int]  # 终局 attempt：expected_artifact_set 复算 content_hash 用
    task_status: str
    execution_log: list[ExecutionRecord]
    kills_done: int
    cancels_done: int
    combos_done: int
    seed: int


class _RecordingExecutor:
    """包住 ScriptedExecutor 记执行底账；仅正常返回才落账（中断不算完整执行）。"""

    def __init__(self, inner: ScriptedExecutor, log: list[ExecutionRecord]) -> None:
        self._inner = inner
        self._log = log

    async def execute(self, ctx: NodeContext) -> NodeOutcome:
        outcome = await self._inner.execute(ctx)
        self._log.append(
            ExecutionRecord("execute", ctx.node_id, ctx.attempt, type(outcome).__name__)
        )
        return outcome

    async def resume(self, ctx: NodeContext, from_step: int) -> NodeOutcome:
        outcome = await self._inner.resume(ctx, from_step)
        self._log.append(
            ExecutionRecord("resume", ctx.node_id, ctx.attempt, type(outcome).__name__)
        )
        return outcome


class _RecordingRegistry:
    def __init__(self, executor: _RecordingExecutor) -> None:
        self._executor = executor

    def resolve(self, role: RoleName) -> tuple[NodeExecutor, ContractPair, BudgetHint]:
        return (
            self._executor,
            ContractPair(),
            BudgetHint(est_tokens=10, est_yuan=Decimal("0.01")),
        )


_MK_TASK_SQL = text(
    """
    INSERT INTO research_tasks
        (id, title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap,
         requested_by)
    VALUES (:id, 'synthetic', 'adversarial', 'EXECUTING', 'sha256:synthetic', 1000000,
            1000.00, 'simrig')
    """
)

_COUNTS_SQL = text(
    """
    SELECT count(*) FILTER (WHERE status = 'RUNNING') AS running,
           count(*) FILTER (WHERE status = 'READY') AS ready
    FROM plan_nodes WHERE task_id = :t
    """
)

_STATUSES_SQL = text("SELECT id, status, attempt FROM plan_nodes WHERE task_id = :t")

_RUNNING_ROWS_SQL = text(
    """
    SELECT id, lease_owner FROM plan_nodes
    WHERE task_id = :t AND status = 'RUNNING' AND cancel_requested_at IS NULL
    """
)

_EXPIRE_OWNER_SQL = text(
    """
    UPDATE plan_nodes SET lease_expires_at = now() - interval '1 second'
    WHERE lease_owner = :owner AND status = 'RUNNING'
    """
)

_EXPIRE_NODE_SQL = text(
    """
    UPDATE plan_nodes SET lease_expires_at = now() - interval '1 second'
    WHERE id = :id AND status = 'RUNNING'
    """
)


class _WorkerSlot(NamedTuple):
    worker: Worker
    task: asyncio.Task[None]


class SimRig:
    """run_to_quiescence：跑到"无 RUNNING、无 READY、注入清账"的稳定静默。

    静默 ≠ 全终态：永败/超时节点的下游滞留 PENDING 是 M1 预期（等 M2 重规划）。
    崩溃注入 = task.cancel 该 worker 协程（连接回滚 ≈ kill -9 的 DB 视角）→ 其持有
    租约置过期（冻结区 2.5 #21 制造法）→ 补一个新 worker（模拟 docker start 重启，
    chaos 演练同口径）。超时剧本靠压缩的真实 node_timeout 判死（asyncio.timeout
    走真实时钟——M1.10 既定口径在此被利用）。
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        node_timeout_seconds: int = 1,
        step_seconds: float = 2.0,
        io_pause: float = 0.02,
        max_iters: int = 2000,  # 超时剧本烧真实秒（每节点 ~2s≈100 迭代），预算给足
    ) -> None:
        self._engine = engine
        self._factory = build_sessionmaker(engine)
        self._tuning = EngineTuning(node_timeout_seconds=node_timeout_seconds)
        self._step = step_seconds
        self._io_pause = io_pause
        self._max_iters = max_iters

    async def run_to_quiescence(self, *, n_workers: int, params: GraphParams) -> SimResult:
        graph, script = generate(params)
        async with self._factory() as session:
            await session.execute(_MK_TASK_SQL, {"id": graph.task_id})
            await session.commit()
            await persist_graph(session, graph, plan_version_added=0)

        clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
        log: list[ExecutionRecord] = []
        registry = _RecordingRegistry(
            _RecordingExecutor(ScriptedExecutor(script, seed=params.seed), log)
        )
        slots: list[_WorkerSlot] = []

        def _spawn() -> None:
            worker = Worker(
                session_factory=self._factory,
                registry=registry,
                clock=clock,
                rng=random.Random(params.seed + len(slots)),
                tuning=self._tuning,
                concurrency=4,
                hooks=NullBudgetHooks(),
            )
            slots.append(_WorkerSlot(worker, asyncio.create_task(worker.run_forever())))

        for _ in range(n_workers):
            _spawn()

        # 注入计划：iter 3 起逐迭代依次发起（时点确定；无对象走 defer/drop）——
        # 图寿命随虚拟/真实节奏漂移，随机时间轴会让后发注入落空，锚早不锚散。
        # rng 只用于注入对象选择（同 seed 同选择序）
        rng = random.Random(params.seed ^ 0x5EED)
        plan: dict[int, list[str]] = {}
        next_at = 3
        for _ in range(params.crash_injections):
            plan.setdefault(next_at, []).append("kill")
            next_at += 1
        for _ in range(params.cancel_injections):
            plan.setdefault(next_at, []).append("cancel")
            next_at += 1
        for _ in range(params.cancel_crash_combo):
            plan.setdefault(next_at, []).append("combo")
            next_at += 1
        kills = cancels = combos = 0

        try:
            stable = 0
            for iter_no in range(self._max_iters):
                for slot in slots:  # 哨兵：worker 永不正常返回，done 且非被杀 = 崩溃
                    if slot.task.done() and not slot.task.cancelled():
                        exc = slot.task.exception()
                        if exc is not None:
                            raise AssertionError(
                                f"worker 崩溃（seed={params.seed}, iter={iter_no}）"
                            ) from exc
                for action in plan.pop(iter_no, []):
                    verdict = await self._inject(action, graph, rng, slots, _spawn)
                    if verdict == "defer":
                        plan.setdefault(iter_no + 3, []).append(action)
                    elif verdict == "done":
                        kills += action == "kill"
                        cancels += action == "cancel"
                        combos += action == "combo"
                running, ready = await self._counts(graph.task_id)
                if running == 0 and ready == 0 and not plan:
                    stable += 1
                    if stable >= 3:  # 连续三拍静默：躲开"DB 已静、worker 在途提交"的窗口
                        break
                else:
                    stable = 0
                clock.advance(self._step)
                await asyncio.sleep(self._io_pause)
            else:
                statuses = await self._statuses(graph.task_id)
                scene = render_ascii(graph, statuses)
                raise AssertionError(f"SimRig 未静默（seed={params.seed}）：\n{scene}")
        finally:
            for slot in slots:
                slot.task.cancel()
            for slot in slots:
                with suppress(asyncio.CancelledError):
                    await slot.task

        async with self._factory() as session:
            rows = (await session.execute(_STATUSES_SQL, {"t": graph.task_id})).all()
            task_status = (
                await session.execute(
                    text("SELECT status FROM research_tasks WHERE id = :t"),
                    {"t": graph.task_id},
                )
            ).scalar_one()
        return SimResult(
            task_id=graph.task_id,
            graph=graph,
            script=script,
            statuses={NodeId(r.id): str(r.status) for r in rows},
            attempts={NodeId(r.id): int(r.attempt) for r in rows},
            task_status=str(task_status),
            execution_log=log,
            kills_done=kills,
            cancels_done=cancels,
            combos_done=combos,
            seed=params.seed,
        )

    async def _counts(self, task_id: TaskId) -> tuple[int, int]:
        async with self._factory() as session:
            row = (await session.execute(_COUNTS_SQL, {"t": task_id})).one()
            return int(row.running), int(row.ready)

    async def _statuses(self, task_id: TaskId) -> dict[NodeId, str]:
        async with self._factory() as session:
            res = await session.execute(_STATUSES_SQL, {"t": task_id})
            return {NodeId(r.id): str(r.status) for r in res}

    async def _inject(
        self,
        action: str,
        graph: PlanGraph,
        rng: random.Random,
        slots: list[_WorkerSlot],
        spawn: Callable[[], None],
    ) -> str:
        statuses = await self._statuses(graph.task_id)
        live = {"PENDING", "READY", "RUNNING"}
        if all(status not in live for status in statuses.values()):
            return "drop"  # 图已收敛：注入对象不存在，放弃（SimResult 计数如实反映）
        if action == "kill":
            victims = [s for s in slots if not s.task.done()]
            if not victims:
                return "defer"
            await self._kill_slot(rng.choice(victims), spawn)
            return "done"
        if action == "cancel":
            candidates = [n.id for n in graph.nodes if statuses.get(n.id) in live]
            if not candidates:
                return "drop"
            root = rng.choice(candidates)
            async with self._factory() as session:
                ids = await subtree_ids(session, root)
            await request_cancel(
                self._factory,
                task_id=graph.task_id,
                node_ids=ids,
                reason="synthetic-cancel",
                hooks=NullBudgetHooks(),
            )
            return "done"
        # combo：取消意图落库后、终态前硬杀持有 worker → 只能靠 reaper 分支③收敛
        async with self._factory() as session:
            rows = (await session.execute(_RUNNING_ROWS_SQL, {"t": graph.task_id})).all()
        if not rows:
            running, ready = await self._counts(graph.task_id)
            if running == 0 and ready == 0:
                return "drop"  # 无 RUNNING 且无供给（滞留 PENDING 被失败堵死）：combo 无对象
            return "defer"
        row = rng.choice(rows)
        # 子树取消（生产语义；单点取消会留下"父取消子滞留"的违规孤儿）——
        # 分支③的打击点在"该 RUNNING 节点带意图 + owner 被杀 + 租约过期"，不受影响
        async with self._factory() as session:
            combo_ids = await subtree_ids(session, NodeId(row.id))
        await request_cancel(
            self._factory,
            task_id=graph.task_id,
            node_ids=combo_ids,
            reason="synthetic-combo",
            hooks=NullBudgetHooks(),
        )
        owner_slot = next(
            (s for s in slots if not s.task.done() and s.worker._worker_id == row.lease_owner),
            None,
        )
        if owner_slot is not None:
            await self._kill_slot(owner_slot, spawn)
        async with self._factory() as session:
            await session.execute(_EXPIRE_NODE_SQL, {"id": row.id})
            await session.commit()
        return "done"

    async def _kill_slot(self, slot: _WorkerSlot, spawn: Callable[[], None]) -> None:
        slot.task.cancel()  # 硬杀协程：在途节点任务连锁取消、连接回滚 ≈ kill -9 的 DB 视角
        with suppress(asyncio.CancelledError):
            await slot.task
        async with self._factory() as session:  # 其持有租约立即置过期
            await session.execute(_EXPIRE_OWNER_SQL, {"owner": slot.worker._worker_id})
            await session.commit()
        spawn()  # 补位新 worker（模拟 docker start 重启；chaos 演练同口径）


# ---------------------------------------------------------------------------
# 13c · 终局不变量断言器（03 §10.1 断言集的 M1 形态 + §10.3 工件集合比对）
# ---------------------------------------------------------------------------

_INVARIANT_ROWS_SQL = text(
    """
    SELECT id, status, attempt, checkpoint_artifact_id, finished_at, cancel_requested_at
    FROM plan_nodes WHERE task_id = :t
    """
)

_ARTIFACT_ROWS_SQL = text(
    """
    SELECT producer_node, schema_name, schema_version, content_hash, kind
    FROM artifacts WHERE task_id = :t
    """
)

_EXPIRED_RUNNING_SQL = text(  # I4：过期超一个 reaper 周期（15s）仍 RUNNING = 悬挂
    """
    SELECT count(*) FROM plan_nodes
    WHERE task_id = :t AND status = 'RUNNING'
      AND lease_expires_at < now() - interval '15 seconds'
    """
)

_TERMINAL = {"DONE", "FAILED", "CANCELLED"}


async def assert_invariants(
    engine: AsyncEngine,
    task_id: TaskId,
    execution_log: Sequence[ExecutionRecord] | None = None,
    *,
    expected_artifacts: set[tuple[NodeId, str, str]] | None = None,
) -> None:
    """终局断言 = 03 §10.1 断言集的 M1 形态（序无关；chaos 演练 M1.14 复用）。

    I1/I2 结构合法（恢复自检口径）；I3 空断言（无预算池，M2 接入）；
    I4 无过期悬挂 RUNNING；I5 DONE 必有检查点工件（"DONE 不可二次变更"由
    M1.2 触发器机制保证，schema 测试已覆盖，此处不重验）；I6 任务终态无活跃行；
    I7 零双执行（执行日志 + 工件侧：DONE 恰一件、非 DONE 零件）。
    """
    factory = build_sessionmaker(engine)
    async with factory() as session:
        graph = await load_graph(session, task_id)
        rows = (await session.execute(_INVARIANT_ROWS_SQL, {"t": task_id})).all()
        arts = (await session.execute(_ARTIFACT_ROWS_SQL, {"t": task_id})).all()
        expired_running = (await session.execute(_EXPIRED_RUNNING_SQL, {"t": task_id})).scalar_one()
        task_status = (
            await session.execute(
                text("SELECT status FROM research_tasks WHERE id = :t"), {"t": task_id}
            )
        ).scalar_one()

    by_id = {NodeId(r.id): r for r in rows}
    assert set(by_id) == {n.id for n in graph.nodes}, "图与节点行集不一致"

    # I1/I2：终局图 + 状态视角过 validate_graph（恢复自检口径）
    statuses = {nid: NodeStatus(r.status) for nid, r in by_id.items()}
    report = validate_graph(graph, statuses=statuses)
    assert report.ok, f"I1/I2 violated: {[str(v.code) for v in report.violations]}"

    # I4：无"RUNNING 且租约过期超一个 reaper 周期"的悬挂行
    assert int(expired_running) == 0, "I4: dangling expired RUNNING beyond reaper period"

    # I5 + I7 工件侧：DONE 必有检查点且恰一件产出；非 DONE 零产出（早期 attempt 无工件）
    per_node = Counter(NodeId(a.producer_node) for a in arts)
    for nid, r in by_id.items():
        if str(r.status) == "DONE":
            assert r.checkpoint_artifact_id is not None, f"I5: DONE {nid} 无检查点"
            assert r.finished_at is not None, f"I5: DONE {nid} 无 finished_at"
            assert per_node.get(nid, 0) == 1, f"I7: DONE {nid} 工件数 {per_node.get(nid, 0)}"
        else:
            assert per_node.get(nid, 0) == 0, f"I7: {r.status} {nid} 却有产出工件"

    # I6：任务终态 → 无 RUNNING/READY 行（预算预留部分 M2 接入）
    if str(task_status) in {"DONE", "DONE_DEGRADED", "FAILED", "CANCELLED"}:
        active = [nid for nid, r in by_id.items() if str(r.status) in {"RUNNING", "READY"}]
        assert not active, f"I6: 任务终态仍有活跃行 {active}"

    # I7 执行日志侧：不存在同 (node_id, attempt) 的两次完整执行
    if execution_log is not None:
        counts = Counter((rec.node_id, rec.attempt) for rec in execution_log)
        dup = {key: n for key, n in counts.items() if n > 1}
        assert not dup, f"I7: 双执行 {dup}"

    # 工件集合一致（03 §10.3）：按 (producer, schema@ver, content_hash) 集合比对，不比顺序
    if expected_artifacts is not None:
        got = {
            (
                NodeId(a.producer_node),
                f"{a.schema_name}@{a.schema_version}",
                str(a.content_hash),
            )
            for a in arts
        }
        assert got == expected_artifacts, (
            f"工件集合漂移：多 {got - expected_artifacts}，少 {expected_artifacts - got}"
        )


def expected_artifact_set(result: SimResult) -> set[tuple[NodeId, str, str]]:
    """由终局推演预期工件集：每个 DONE 节点恰一件，content_hash 按剧本口径复算。

    ScriptedExecutor 的 payload = {node, attempt, seed} 确定可复算（03 §10.3 的
    并发回放集合比对靠它）；sink 的 kind 不同但 schema/payload 口径一致。
    """
    expected: set[tuple[NodeId, str, str]] = set()
    for node in result.graph.nodes:
        if result.statuses.get(node.id) != "DONE":
            continue
        payload = {
            "node": str(node.id),
            "attempt": result.attempts[node.id],
            "seed": result.seed,
        }
        expected.add((node.id, "scripted_note@1", ArtifactDraft.hash_payload(payload)))
    return expected
