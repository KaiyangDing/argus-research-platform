"""合成图生成器与对抗执行台（03 §10.1/§10.2/§10.3；M1 册 M1.13，[AI写]）。

13a：参数化生成器 + DAG 文本渲染（本份）；13b：SimRig 注入执行台；13c：不变量断言器。

确定性口径：一切结构决策（分层/连边/纯度/剧本）吃 GraphParams.seed；节点 id 每次
generate 新生成（PK 不可复用），跨次比对按**生成序索引**对齐。
surgery 注入是 M2 接入位（冻结区 2.5 #17），M1 恒缺席。
"""

import random
import uuid
from collections.abc import Mapping
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from argus.core.types import ArtifactKind, ContractId, NodeId, RoleName, TaskId
from argus.engine.graph import (
    BudgetRequest,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from tests.support.scripted_executor import ScriptEntry

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
        elif rng.random() < params.slow_ratio:
            secs = rng.uniform(*params.slow_seconds)
            script[(node.id, 1)] = ScriptEntry(action="hang", hang_seconds=secs)
        elif node.purity is Purity.EFFECTFUL:
            steps = rng.randint(2, 4)
            for attempt in (1, 2):  # 同步骤结构：崩溃后 attempt 2 走 resume 续跑
                script[(node.id, attempt)] = ScriptEntry(action="effectful_steps", steps=steps)
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
