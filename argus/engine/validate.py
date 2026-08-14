"""图不变量校验器：I1/I2 全量 + I3 纯图检查（03 §1.1/§2.4 的 M1 子集）。

纯函数、零 LLM、零 IO（03 §1.1 铁律）。三时机复用（计划编译/落库前/恢复自检）；
I3 的父子池层级与合成保底 M2 接入，本步只冻结签名与违规码。
"""

from collections import defaultdict, deque
from collections.abc import Mapping
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict

from argus.core.types import CONTRACT_ID_RE, NodeId
from argus.engine.graph import BudgetRequest, NodeStatus, PlanEdge, PlanGraph


class ViolationCode(StrEnum):
    """结构化违规码（M1 册【实现细则】；I 编号对应 03 §2.4）。"""

    CYCLE = "I1_cycle"
    ORPHAN = "I2_orphan"
    DANGLING_EDGE = "I2_dangling_edge"
    BUDGET_OVER_CAP = "I3_budget_over_cap"
    CONTRACT_SHAPE = "contract_shape"  # ContractId 形状（注册表校验 M3 接入）
    OUT_EDGE_CONTRACT_MIXED = "out_edge_contract_mixed"  # 同 src 出边契约不一致


class Violation(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: ViolationCode
    node_ids: tuple[NodeId, ...] = ()
    detail: str


class Warning_(BaseModel):
    """I2 注²：前驱 FAILED 未处置是修复中间态——警告不是违规（03 §2.4）。"""

    model_config = ConfigDict(frozen=True)

    code: Literal["I2_failed_predecessor_pending"] = "I2_failed_predecessor_pending"
    node_ids: tuple[NodeId, ...]


class ValidationReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    violations: tuple[Violation, ...]
    warnings: tuple[Warning_, ...]

    @property
    def ok(self) -> bool:
        return not self.violations


def validate_graph(
    graph: PlanGraph,
    *,
    statuses: Mapping[NodeId, NodeStatus] | None = None,
    task_caps: BudgetRequest | None = None,
) -> ValidationReport:
    """statuses=None 即计划编译时（全 PENDING 视角）；task_caps=None 跳过 I3。"""
    violations: list[Violation] = []
    warnings: list[Warning_] = []
    known = {n.id for n in graph.nodes}

    # I2a 悬空边：端点必须存在；后续结构检查只吃两端俱在的边
    edges: list[PlanEdge] = []
    for e in graph.edges:
        if e.src not in known or e.dst not in known:
            violations.append(
                Violation(code=ViolationCode.DANGLING_EDGE, detail=f"{e.src} -> {e.dst}")
            )
        else:
            edges.append(e)

    # 契约形状（纵深防御：覆盖绕过 pydantic 构造 / 库中脏数据的图）
    for e in graph.edges:
        if CONTRACT_ID_RE.match(e.contract) is None:
            violations.append(
                Violation(
                    code=ViolationCode.CONTRACT_SHAPE,
                    node_ids=(e.src,) if e.src in known else (),
                    detail=f"illegal contract id: {e.contract!r}",
                )
            )

    # 同 src 出边契约必须一致（冻结区 2.5 #8：输出契约存 src.spec，一节点一契约）
    by_src: defaultdict[NodeId, set[str]] = defaultdict(set)
    for e in edges:
        by_src[e.src].add(e.contract)
    for src, contracts in by_src.items():
        if len(contracts) > 1:
            violations.append(
                Violation(
                    code=ViolationCode.OUT_EDGE_CONTRACT_MIXED,
                    node_ids=(src,),
                    detail=f"{sorted(contracts)}",
                )
            )

    # I1 Kahn 拓扑排序：处理不完的剩余节点即环成员（环下游滞留节点一并列入）
    indeg = {n.id: 0 for n in graph.nodes}
    succs: defaultdict[NodeId, list[NodeId]] = defaultdict(list)
    for e in edges:
        indeg[e.dst] += 1
        succs[e.src].append(e.dst)
    queue = deque(nid for nid, d in indeg.items() if d == 0)
    remaining = set(indeg)
    while queue:
        nid = queue.popleft()
        remaining.discard(nid)
        for succ in succs[nid]:
            indeg[succ] -= 1
            if indeg[succ] == 0:
                queue.append(succ)
    if remaining:
        violations.append(
            Violation(
                code=ViolationCode.CYCLE,
                node_ids=tuple(sorted(remaining, key=str)),
                detail=f"{len(remaining)} node(s) stuck in cycle",
            )
        )

    # I2b 可达性：根 = 入度 0——悬空入边也算入度（来路不明的节点不是根）；
    # 环滞留节点已由 CYCLE 覆盖，不重复报
    indeg_all = {n.id: 0 for n in graph.nodes}
    for e in graph.edges:
        if e.dst in known:
            indeg_all[e.dst] += 1
    reached: set[NodeId] = set()
    stack = [nid for nid, d in indeg_all.items() if d == 0]
    while stack:
        nid = stack.pop()
        if nid in reached:
            continue
        reached.add(nid)
        stack.extend(succs[nid])
    orphans = known - reached - remaining
    if orphans:
        violations.append(
            Violation(
                code=ViolationCode.ORPHAN,
                node_ids=tuple(sorted(orphans, key=str)),
                detail="unreachable from any root",
            )
        )

    # 恢复自检视角：PENDING/READY 的前驱非 CANCELLED（违规，复用 ORPHAN 码——
    # 永无促升可能即执行意义的孤儿）；前驱 FAILED → 警告（03 §2.4 注²）
    if statuses is not None:
        preds: defaultdict[NodeId, list[NodeId]] = defaultdict(list)
        for e in edges:
            preds[e.dst].append(e.src)
        failed_pending: set[NodeId] = set()
        for n in graph.nodes:
            if statuses.get(n.id) not in (NodeStatus.PENDING, NodeStatus.READY):
                continue
            for p in preds[n.id]:
                if statuses.get(p) is NodeStatus.CANCELLED:
                    violations.append(
                        Violation(
                            code=ViolationCode.ORPHAN,
                            node_ids=(n.id,),
                            detail="predecessor CANCELLED, node can never be promoted",
                        )
                    )
                elif statuses.get(p) is NodeStatus.FAILED:
                    failed_pending.add(n.id)
        if failed_pending:
            warnings.append(Warning_(node_ids=tuple(sorted(failed_pending, key=str))))

    # I3：Σ节点申请 ≤ 任务根 cap（父子池层级与合成保底 M2 接入）
    if task_caps is not None:
        total_tokens = sum(n.budget.tokens for n in graph.nodes)
        total_yuan = sum((n.budget.yuan for n in graph.nodes), Decimal("0"))
        if total_tokens > task_caps.tokens or total_yuan > task_caps.yuan:
            violations.append(
                Violation(
                    code=ViolationCode.BUDGET_OVER_CAP,
                    detail=f"Σtokens={total_tokens}/{task_caps.tokens} "
                    f"Σyuan={total_yuan}/{task_caps.yuan}",
                )
            )

    return ValidationReport(violations=tuple(violations), warnings=tuple(warnings))
