"""worker 主循环与节点执行（03 §4.2/§3.5；M1 册 M1.10）。

进程入口：python -m argus.engine.worker --registry <module:factory>（仅测试/演练注入；
生产注册表 M3 由 roster 提供）。引擎调参收进 EngineTuning 冻结类，不新增环境变量
（速查表 H.2 是 ARGUS_ 全集，扩集是契约变更）。
"""

import argparse
import asyncio
import importlib
import os
import random
import secrets
import socket
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.config import get_settings
from argus.core.db import build_engine, build_sessionmaker
from argus.engine.cancel import CancelToken
from argus.engine.clock import Clock, SystemClock
from argus.engine.graph import FailureClass, Purity
from argus.engine.lease import (
    TerminalKind,
    WorkerGuard,
    commit_cancelled,
    commit_done,
    commit_failed,
    commit_needs_replan,
    commit_retry,
    decide_terminal,
    heartbeat,
)
from argus.engine.ports import (
    BudgetHooks,
    ExecutorRegistry,
    NodeContext,
    NodeExecutor,
    NodeOutcome,
    NullBudgetHooks,
    NullBudgetView,
    OutcomeDegraded,
    OutcomeFailure,
    OutcomeSuccess,
    ReplanSignalDraft,
)
from argus.engine.reaper import reap_once
from argus.engine.scheduler import ClaimedNode, claim_batch, compute_batch
from argus.engine.steps import StepJournal

_LOG = structlog.get_logger(__name__)


@dataclass(frozen=True)
class EngineTuning:
    """引擎调参冻结类（M1 册 2.4；默认值出处速查表 D.2）。"""

    lease_ttl_seconds: int = 90
    heartbeat_seconds: int = 30  # lease_ttl/3
    reaper_period_seconds: int = 15
    t_grace_seconds: int = 10
    node_timeout_seconds: int = 600  # 走真实时钟（asyncio.timeout）；测试调小、不伪造适配
    tick_min_seconds: float = 0.1
    tick_max_seconds: float = 0.5


def make_worker_id() -> str:
    """{hostname}-{pid}-{8位随机hex}（03 §2.2）；随机段只为身份唯一性，非逻辑随机。"""
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(4)}"


def _error_payload(exc: BaseException) -> dict[str, Any]:
    """结构化 traceback 摘要（03 §4.2 ⑤）：类型 + 消息 + 截断栈。"""
    return {
        "type": type(exc).__name__,
        "message": str(exc)[:500],
        "traceback": "".join(traceback.format_exception(exc, limit=8))[-2000:],
    }


class Worker:
    """单进程 worker：主循环领取派发 + 每节点心跳/超时/收尾（03 §4.2 逐行）。"""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        registry: ExecutorRegistry,
        clock: Clock,
        rng: random.Random,
        tuning: EngineTuning,
        concurrency: int,
        hooks: BudgetHooks,
    ) -> None:
        self._factory = session_factory
        self._registry = registry
        self._clock = clock
        self._rng = rng
        self._tuning = tuning
        self._concurrency = concurrency
        self._hooks = hooks
        self._worker_id = make_worker_id()
        self._semaphore = asyncio.Semaphore(concurrency)
        self._in_flight = 0
        self._last_reap: datetime | None = None

    async def run_forever(self) -> None:
        """主循环：reap → 批量自适应领取 → 派发 → 抖动 tick（ADR-004 短轮询）。"""
        async with asyncio.TaskGroup() as tg:
            while True:
                await self._reap_once()
                batch = compute_batch(self._concurrency, self._in_flight)
                claimed: list[ClaimedNode] = []
                if batch > 0:
                    claimed = await claim_batch(
                        self._factory,
                        worker_id=self._worker_id,
                        batch=batch,
                        lease_ttl_seconds=self._tuning.lease_ttl_seconds,
                        hooks=self._hooks,
                    )
                for node in claimed:
                    self._in_flight += 1
                    tg.create_task(self._run_node_guarded(node))
                await self._clock.sleep(
                    self._rng.uniform(self._tuning.tick_min_seconds, self._tuning.tick_max_seconds)
                )

    async def _reap_once(self) -> None:
        """内置 reaper（03 §3.3）：按 reaper_period 节流，经 Clock 计时（FakeClock 可驱动）。"""
        now = self._clock.now()
        if (
            self._last_reap is not None
            and (now - self._last_reap).total_seconds() < self._tuning.reaper_period_seconds
        ):
            return
        self._last_reap = now
        await reap_once(self._factory, hooks=self._hooks)

    async def _run_node_guarded(self, claimed: ClaimedNode) -> None:
        try:
            async with self._semaphore:  # 03 §3.5 并发上限（与批量自适应双保险）
                await self._run_node(claimed)
        finally:
            self._in_flight -= 1

    async def _run_node(self, claimed: ClaimedNode) -> None:
        executor, _contracts, _hint = self._registry.resolve(claimed.role)
        journal: StepJournal | None = None
        if claimed.purity is Purity.EFFECTFUL:
            journal = StepJournal(self._factory, node_id=claimed.node_id, attempt=claimed.attempt)
        token = CancelToken()
        ctx = NodeContext(
            task_id=claimed.task_id,
            node_id=claimed.node_id,
            attempt=claimed.attempt,
            brief=claimed.spec.brief,
            inputs=claimed.spec.inputs,
            budget=NullBudgetView(),
            cancel_token=token,
            step_journal=journal,
            clock=self._clock,
        )
        lease_lost = False

        async def _hb_loop() -> None:
            nonlocal lease_lost
            while True:
                await self._clock.sleep(self._tuning.heartbeat_seconds)
                hb = await heartbeat(
                    self._factory,
                    node_id=claimed.node_id,
                    worker_id=self._worker_id,
                    lease_ttl_seconds=self._tuning.lease_ttl_seconds,
                )
                if not hb.alive:
                    lease_lost = True
                    exec_task.cancel()  # 租约易主：杀执行、丢弃一切、不提交（03 §3.3）
                    return
                if hb.cancel_requested:
                    token.set()  # 取消收敛 M1.12 消费；本步先传递信号

        pure_cancelled = False
        grace_exceeded = False

        async def _cancel_watch() -> None:
            nonlocal pure_cancelled, grace_exceeded
            await token.wait()
            if claimed.purity is Purity.PURE:
                pure_cancelled = True
                exec_task.cancel()  # PURE：硬杀无害（03 §7.2）
                return
            # EFFECTFUL：等步骤边界自行返回，上限 T_grace（经 clock）
            await self._clock.sleep(self._tuning.t_grace_seconds)
            if not exec_task.done():
                grace_exceeded = True
                exec_task.cancel()

        exec_task = asyncio.create_task(self._execute(ctx, executor, journal))
        hb_task = asyncio.create_task(_hb_loop())
        watch_task = asyncio.create_task(_cancel_watch())
        outcome: NodeOutcome
        guard = WorkerGuard(worker_id=self._worker_id, attempt=claimed.attempt)
        try:
            outcome = await exec_task
        except asyncio.CancelledError:
            if lease_lost:
                return  # 租约易主：静默丢弃
            if grace_exceeded:
                _LOG.warning(
                    "cancel_grace_exceeded",
                    node_id=str(claimed.node_id),
                    grace_seconds=self._tuning.t_grace_seconds,
                )
                await commit_failed(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    failure_class=FailureClass.CANCEL_TIMEOUT,
                    error={"cause": "cancel_grace_exceeded"},
                    signal=None,
                    hooks=self._hooks,
                )
                return
            if pure_cancelled:
                await commit_cancelled(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    hooks=self._hooks,
                )
                return
            raise  # 外部关停不吞（03 §7.2）
        except Exception as exc:  # noqa: BLE001 —— 03 §4.2⑤：执行体任意异常→结构化可重试失败
            outcome = OutcomeFailure(retryable=True, error=_error_payload(exc))
        finally:
            hb_task.cancel()
            watch_task.cancel()
        await self._commit(claimed, outcome, cancel_requested=token.is_set())

    async def _execute(
        self, ctx: NodeContext, executor: NodeExecutor, journal: StepJournal | None
    ) -> NodeOutcome:
        """恢复即常态路径（03 §4.2）：EFFECTFUL 有日志走 resume，其余 execute。"""
        async with asyncio.timeout(self._tuning.node_timeout_seconds):
            if journal is not None and await journal.exists():
                return await executor.resume(ctx, from_step=await journal.last_done())
            return await executor.execute(ctx)

    async def _commit(
        self, claimed: ClaimedNode, outcome: NodeOutcome, *, cancel_requested: bool
    ) -> None:
        kind = decide_terminal(
            outcome,
            attempt=claimed.attempt,
            max_attempts=claimed.max_attempts,
            replan_on_failure=claimed.spec.replan_on_failure,
            cancel_requested=cancel_requested,
        )
        guard = WorkerGuard(worker_id=self._worker_id, attempt=claimed.attempt)
        match kind:
            case TerminalKind.DONE:
                ok = cast(OutcomeSuccess | OutcomeDegraded, outcome)  # 判定表保证配对
                await commit_done(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    artifact=ok.artifact,
                    signal=ok.replan_signal,
                    hooks=self._hooks,
                )
            case TerminalKind.RETRY:
                failure = cast(OutcomeFailure, outcome)
                retried = await commit_retry(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    error=failure.error,
                )
                if not retried:
                    # C-7 族④：意图落库但心跳未及置 token 时，判定表看不到取消——
                    # T3b 新守卫拒绝带意图行回队，在事实源上重执行"取消压倒重试"走 T7；
                    # 纯 fencing 场景下 T7 守卫同样 rowcount=0，无害
                    await commit_cancelled(
                        self._factory,
                        task_id=claimed.task_id,
                        node_id=claimed.node_id,
                        guard=guard,
                        hooks=self._hooks,
                    )
            case TerminalKind.FAILED:
                failure = cast(OutcomeFailure, outcome)
                # failure_class 为 None 的可达路径只有"可重试异常耗尽额度"（03-T5）
                await commit_failed(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    failure_class=failure.failure_class or FailureClass.RETRY_EXHAUSTED,
                    error=failure.error,
                    signal=failure.replan_signal,
                    hooks=self._hooks,
                )
            case TerminalKind.NEEDS_REPLAN:
                failure = cast(OutcomeFailure, outcome)
                if failure.replan_signal is not None:  # T6a：无产出、只带信号
                    await commit_needs_replan(
                        self._factory,
                        task_id=claimed.task_id,
                        node_id=claimed.node_id,
                        guard=guard,
                        signal=failure.replan_signal,
                        error=None,
                        failure_class=None,
                        hooks=self._hooks,
                    )
                else:  # T6b：重试耗尽转重规划
                    await commit_needs_replan(
                        self._factory,
                        task_id=claimed.task_id,
                        node_id=claimed.node_id,
                        guard=guard,
                        signal=ReplanSignalDraft(
                            kind="node_replan", payload={"cause": "retry_exhausted"}
                        ),
                        error=failure.error,
                        failure_class=FailureClass.RETRY_EXHAUSTED,
                        hooks=self._hooks,
                    )
            case TerminalKind.CANCELLED:
                # 取消后产物一律丢弃走 T7（冻结区 2.5 #25）——不为迟到的成功开后门
                await commit_cancelled(
                    self._factory,
                    task_id=claimed.task_id,
                    node_id=claimed.node_id,
                    guard=guard,
                    hooks=self._hooks,
                )


async def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m argus.engine.worker",
        description="Argus 任务图 worker（M1：注册表仅测试/演练注入，生产 M3 由 roster 提供）",
    )
    parser.add_argument(
        "--registry", required=True, help="module.path:factory，factory() -> ExecutorRegistry"
    )
    args = parser.parse_args(argv)
    module_name, _, factory_name = args.registry.partition(":")
    if not module_name or not factory_name:
        parser.error("--registry 形如 module.path:factory_name")
    registry: ExecutorRegistry = getattr(importlib.import_module(module_name), factory_name)()
    settings = get_settings()
    engine = build_engine(settings)
    try:
        worker = Worker(
            session_factory=build_sessionmaker(engine),
            registry=registry,
            clock=SystemClock(),
            rng=random.Random(),
            tuning=EngineTuning(),
            concurrency=settings.worker_concurrency,
            hooks=NullBudgetHooks(),
        )
        await worker.run_forever()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
