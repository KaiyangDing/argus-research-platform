"""M1.9 EFFECTFUL 步骤日志与断点续跑测试（M1 册测试清单，+6，全 [pg]）。"""

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import NodeId, TaskId
from argus.engine.cancel import CancelToken
from argus.engine.graph import BudgetRequest, TaskBrief
from argus.engine.ports import NodeContext, NullBudgetView, OutcomeSuccess
from argus.engine.steps import StepJournal, StepKeyMismatch, StepStateError
from argus.engine.store import NodeSpec
from tests.support.fake_clock import FakeClock
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry

pytestmark = pytest.mark.pg

_SPEC_JSON = json.dumps(
    NodeSpec(
        brief=TaskBrief(objective="o"),
        budget=BudgetRequest(tokens=100, yuan=Decimal("1.0")),
    ).model_dump(mode="json"),
    ensure_ascii=False,
)


async def _seed_node(session: AsyncSession) -> tuple[TaskId, NodeId]:
    task_id = TaskId(uuid.uuid4())
    node_id = NodeId(uuid.uuid4())
    await session.execute(
        text(
            """
            INSERT INTO research_tasks
                (id, title, objective, status, corpus_hash, budget_tokens_cap,
                 budget_yuan_cap, requested_by)
            VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
            """
        ),
        {"id": task_id},
    )
    await session.execute(
        text(
            """
            INSERT INTO plan_nodes
                (id, task_id, plan_version_added, node_type, role, spec, status, attempt,
                 purity)
            VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb),
                    'RUNNING', 1, 'effectful')
            """
        ),
        {"id": node_id, "task": task_id, "spec": _SPEC_JSON},
    )
    await session.commit()
    return task_id, node_id


def _ctx(task: TaskId, node: NodeId, attempt: int, journal: StepJournal | None) -> NodeContext:
    return NodeContext(
        task_id=task,
        node_id=node,
        attempt=attempt,
        brief=TaskBrief(objective="o"),
        inputs=(),
        budget=NullBudgetView(),
        cancel_token=CancelToken(),
        step_journal=journal,
        clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)),
    )


async def _steps(session: AsyncSession, node: NodeId) -> list[tuple[int, str, str | None]]:
    res = await session.execute(
        text(
            "SELECT step_no, status, result_digest FROM node_steps "
            "WHERE node_id = :n ORDER BY step_no"
        ),
        {"n": node},
    )
    return [(r.step_no, r.status, r.result_digest) for r in res]


async def test_begin_persists_intent_before_work(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    _, node = await _seed_node(graph_session)
    journal = StepJournal(build_sessionmaker(pg_engine), node_id=node, attempt=1)
    begun = await journal.begin(1, "download-a")
    assert begun.skip is False
    # begin 返回后立即从另一连接可见：独立小事务已提交，意图先于副作用持久化（03 §4.3）
    assert await _steps(graph_session, node) == [(1, "INTENT", None)]


async def test_complete_marks_done_with_digest(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    _, node = await _seed_node(graph_session)
    journal = StepJournal(build_sessionmaker(pg_engine), node_id=node, attempt=1)
    await journal.begin(1, "download-a")
    await journal.complete(1, "sha:aaa")
    assert await _steps(graph_session, node) == [(1, "DONE", "sha:aaa")]
    with pytest.raises(StepStateError):
        await journal.complete(1, "sha:bbb")  # 重复 complete：行已不在 INTENT 态


async def test_last_done_and_exists(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    _, node = await _seed_node(graph_session)
    journal = StepJournal(build_sessionmaker(pg_engine), node_id=node, attempt=1)
    assert await journal.exists() is False
    assert await journal.last_done() == 0
    await journal.begin(1, "s1")
    await journal.complete(1, "d1")
    await journal.begin(2, "s2")  # 只有意图、未完成
    assert await journal.exists() is True
    assert await journal.last_done() == 1  # 最大 DONE 步；INTENT 不算


async def test_resume_skips_completed_steps(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed_node(graph_session)
    factory = build_sessionmaker(pg_engine)
    executor = ScriptedExecutor(
        {
            (node, 1): ScriptEntry(action="effectful_steps", steps=4, crash_after_step=2),
            (node, 2): ScriptEntry(action="effectful_steps", steps=4),
        },
        seed=42,
    )
    journal1 = StepJournal(factory, node_id=node, attempt=1)
    with pytest.raises(RuntimeError, match="crash after step 2"):
        await executor.execute(_ctx(task, node, 1, journal1))  # 完成 1、2 后崩
    journal2 = StepJournal(factory, node_id=node, attempt=2)
    resume_from = await journal2.last_done()
    assert resume_from == 2
    outcome = await executor.resume(_ctx(task, node, 2, journal2), from_step=resume_from)
    assert isinstance(outcome, OutcomeSuccess)
    rows = await _steps(graph_session, node)
    # 步骤行总数不多不少：1、2 是 attempt1 的（零重做），3、4 是 attempt2 续干的
    assert [(no, st) for no, st, _ in rows] == [
        (1, "DONE"),
        (2, "DONE"),
        (3, "DONE"),
        (4, "DONE"),
    ]


async def test_half_step_intent_retaken_same_key(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    _, node = await _seed_node(graph_session)
    factory = build_sessionmaker(pg_engine)
    journal1 = StepJournal(factory, node_id=node, attempt=1)
    await journal1.begin(1, "upload-x")  # 半截步骤：INTENT 落了、副作用未知、没有完成标记
    journal2 = StepJournal(factory, node_id=node, attempt=2)
    retaken = await journal2.begin(1, "upload-x")
    assert retaken.skip is False  # 同 step_key：接管重试（凭幂等键向副作用属主安全重试）
    await journal2.complete(1, "d1")
    _, node2 = await _seed_node(graph_session)
    journal3 = StepJournal(factory, node_id=node2, attempt=1)
    await journal3.begin(1, "upload-x")
    journal4 = StepJournal(factory, node_id=node2, attempt=2)
    with pytest.raises(StepKeyMismatch):
        await journal4.begin(1, "upload-y")  # 异 key：步骤序错位，必须人查


async def test_pure_node_never_touches_journal(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed_node(graph_session)
    executor = ScriptedExecutor({}, seed=7)  # 缺省 success（PURE 剧本）
    outcome = await executor.execute(_ctx(task, node, 1, None))  # PURE：不注入 journal
    assert isinstance(outcome, OutcomeSuccess)
    assert await _steps(graph_session, node) == []  # 全程 node_steps 零行（03 §4.3 分野）
