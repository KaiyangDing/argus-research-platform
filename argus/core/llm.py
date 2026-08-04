"""LLM 薄客户端：档位选择 + 预算闸门 + 429 退避 + 流中断入账 + embedding + cassette 三模式。

刻意不重建供应商可靠性设施（00 §2.3 不重叠纪律）：429 只退让、5xx 单次重试。
三模式（M0.7 接入）：replay 走带子零 HTTP、record 真实调用并落带、live 真实调用不落带；
stream 暂仅 live（流式 cassette 化待 M1 消费方出现时定义【实现细则】）。
"""

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

import httpx

from argus.core.config import ArgusSettings
from argus.core.logging import get_logger
from argus.core.types import ArgusError, ModelTier

if TYPE_CHECKING:
    from argus.core.cassette import CassettePlayer, CassetteRecorder, CassetteStore

_BACKOFF_DELAYS = (0, 0.5, 1, 2, 4, 8)  # 【实现细则】429 退避序列，无抖动（测试确定性）


def canonical_json(obj: Any) -> str:
    """排序键 + 剔除 None 值：同一请求任何构造顺序得同一串（digest 的口径）。"""

    def _strip(o: Any) -> Any:
        if isinstance(o, dict):
            return {k: _strip(v) for k, v in o.items() if v is not None}
        if isinstance(o, list):
            return [_strip(v) for v in o]
        return o

    return json.dumps(_strip(obj), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def request_digest(body: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(body)).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Message:
    role: Literal["system", "user", "assistant"]
    content: str


@dataclass(frozen=True, slots=True)
class LLMUsage:
    input_tokens: int
    output_tokens: int
    cached_tokens: int = 0  # context cache 命中（M5 观测点，先落字段）
    estimated: bool = False  # True = 供应商 usage 缺失，按已收内容本地估计（03 §7.2）


@dataclass(frozen=True, slots=True)
class LLMResult:
    text: str
    model: str
    request_id: str | None
    usage: LLMUsage
    cost_yuan: Decimal


@dataclass(frozen=True, slots=True)
class CallContext:
    scenario_id: str  # cassette 场景坐标；生产运行 = 任务 id 字符串
    node_id: str  # M0 无真实节点：冒烟用 "smoke"、语料用 "corpus"
    attempt: int = 1
    task_id: str | None = None  # 日志绑定用（02 §7.1）
    role: str | None = None
    # call_index 不在此处：由 client 按 (scenario_id, node_id, attempt) 内部计数分配


class BudgetExceededError(ArgusError):
    def __init__(
        self, gate_name: str, cap_yuan: Decimal, spent_yuan: Decimal, attempted_yuan: Decimal
    ) -> None:
        super().__init__(
            f"预算闸门 {gate_name} 拦截：已花 {spent_yuan} + 预估 {attempted_yuan} > 上限 {cap_yuan}"
        )
        self.gate_name = gate_name
        self.cap_yuan = cap_yuan
        self.spent_yuan = spent_yuan
        self.attempted_yuan = attempted_yuan


class LLMCallError(ArgusError):
    def __init__(
        self, status_code: int | None, *, congestion: bool = False, detail: str = ""
    ) -> None:
        super().__init__(f"LLM 调用失败：status={status_code} congestion={congestion} {detail}")
        self.status_code = status_code
        self.congestion = congestion
        self.detail = detail


class BudgetGate:
    """进程内预算累计器【实现细则·M0 形态】：M2 预算树以同形态接口注入，本类接口不变。"""

    def __init__(self, name: str, cap_yuan: Decimal) -> None:
        self.name = name
        self.cap_yuan = cap_yuan
        self._spent = Decimal(0)

    @property
    def spent_yuan(self) -> Decimal:
        return self._spent

    def precheck(self, est_yuan: Decimal) -> None:
        if self._spent + est_yuan > self.cap_yuan:
            raise BudgetExceededError(self.name, self.cap_yuan, self._spent, est_yuan)

    def settle(self, actual_yuan: Decimal) -> None:
        self._spent += actual_yuan  # 只累计不抛：钱已经花了，账必须记全


@dataclass(frozen=True)
class ModelPrice:
    input_per_1k: Decimal
    output_per_1k: Decimal  # embedding 模型 output=0


# 价目来源：百炼官网价目页，2026-08-04 查询并经控制台核对（aiapiprice 聚合 2026-06-15 版交叉）。
MODEL_PRICES: dict[str, ModelPrice] = {
    "qwen-flash": ModelPrice(input_per_1k=Decimal("0.0012"), output_per_1k=Decimal("0.0072")),
    "qwen-plus": ModelPrice(input_per_1k=Decimal("0.002"), output_per_1k=Decimal("0.008")),
    "qwen-max": ModelPrice(input_per_1k=Decimal("0.012"), output_per_1k=Decimal("0.036")),
    "text-embedding-v4": ModelPrice(input_per_1k=Decimal("0.0005"), output_per_1k=Decimal(0)),
}


class LLMStream:
    """流式结果。aclose() 契约（03 §7.2）：关连接；缺 usage 按已收文本估算折价入账。"""

    def __init__(
        self,
        *,
        response: httpx.Response,
        model: str,
        est_input_tokens: int,
        gates: Sequence[BudgetGate],
        cost_fn: Callable[[LLMUsage], Decimal],
    ) -> None:
        self._response = response
        self._model = model
        self._est_input = est_input_tokens
        self._gates = gates
        self._cost_fn = cost_fn
        self._chunks: list[str] = []
        self._usage: LLMUsage | None = None
        self._request_id: str | None = None
        self._result: LLMResult | None = None

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[str]:
        async for line in self._response.aiter_lines():
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                break
            data = json.loads(payload)
            if self._request_id is None:
                self._request_id = data.get("id")
            usage_raw = data.get("usage")
            if usage_raw:
                self._usage = LLMUsage(
                    input_tokens=int(usage_raw.get("prompt_tokens", 0)),
                    output_tokens=int(usage_raw.get("completion_tokens", 0)),
                )
            choices = data.get("choices") or []
            if choices:
                delta = (choices[0].get("delta") or {}).get("content")
                if delta:
                    self._chunks.append(delta)
                    yield delta
        await self._finalize(closed_early=False)

    async def _finalize(self, *, closed_early: bool) -> None:
        if self._result is not None:
            return
        await self._response.aclose()
        text = "".join(self._chunks)
        if self._usage is not None and not closed_early:
            usage = self._usage
        else:
            usage = LLMUsage(
                input_tokens=self._est_input,
                output_tokens=max(1, len(text) // 2),  # 【实现细则】中文≈2字符/token 粗估
                estimated=True,
            )
        cost = self._cost_fn(usage)
        self._result = LLMResult(
            text=text, model=self._model, request_id=self._request_id, usage=usage, cost_yuan=cost
        )
        for g in self._gates:
            g.settle(cost)

    async def aclose(self) -> None:
        await self._finalize(closed_early=True)

    @property
    def result(self) -> LLMResult:
        if self._result is None:
            raise ArgusError("流未结束：result 仅在流耗尽或 aclose() 后可读")
        return self._result


class LLMClient:
    def __init__(
        self,
        settings: ArgusSettings,
        *,
        gates: Sequence[BudgetGate] = (),
        cassette_store: CassetteStore | None = None,
        transport: httpx.AsyncClient | None = None,
        prices: Mapping[str, ModelPrice] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._settings = settings
        self._gates = list(gates)
        self._store = cassette_store
        self._players: dict[str, CassettePlayer] = {}
        self._recorder: CassetteRecorder | None = None
        self._http = transport or httpx.AsyncClient(timeout=settings.llm_timeout_s)
        self._prices = dict(prices or MODEL_PRICES)
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._log = get_logger("argus.core.llm")
        self._call_counter: dict[tuple[str, str, int], int] = {}

    def model_for(self, tier: ModelTier) -> str:
        return self._settings.model_for(tier)

    def _player_for(self, scenario_id: str) -> CassettePlayer:
        from argus.core.cassette import CassettePlayer  # 延迟导入：保持 cassette→llm 单向

        player = self._players.get(scenario_id)
        if player is None:
            if self._store is None:
                raise ArgusError("replay 模式需要 cassette_store（07 §8）")
            player = CassettePlayer(self._store.load(scenario_id))
            self._players[scenario_id] = player
        return player

    def _replay_entry(self, ctx: CallContext, call_index: int, digest: str) -> LLMResult:
        from argus.core.cassette import CassetteKey

        entry = self._player_for(ctx.scenario_id).replay(
            CassetteKey(node_id=ctx.node_id, attempt=ctx.attempt, call_index=call_index), digest
        )
        usage = LLMUsage(
            input_tokens=entry.usage.get("input", 0),
            output_tokens=entry.usage.get("output", 0),
            cached_tokens=entry.usage.get("cached", 0),
        )
        cost = Decimal(entry.cost_yuan)
        for g in self._gates:
            g.precheck(cost)  # 【实现细则】回放也走闸门：预算行为零费用可测
        for g in self._gates:
            g.settle(cost)
        return LLMResult(
            text=entry.response_text or "",
            model=entry.model,
            request_id=entry.request_id,
            usage=usage,
            cost_yuan=cost,
        )

    def _replay_vectors(self, ctx: CallContext, call_index: int, digest: str) -> list[list[float]]:
        from argus.core.cassette import CassetteKey

        entry = self._player_for(ctx.scenario_id).replay(
            CassetteKey(node_id=ctx.node_id, attempt=ctx.attempt, call_index=call_index), digest
        )
        cost = Decimal(entry.cost_yuan)
        for g in self._gates:
            g.precheck(cost)
        for g in self._gates:
            g.settle(cost)
        return list(entry.response_vectors or [])

    def _recorder_for(self, scenario_id: str) -> CassetteRecorder:
        from argus.core.cassette import CassetteRecorder, CassetteStore

        if self._recorder is None:
            store = self._store or CassetteStore()
            self._recorder = CassetteRecorder(
                store,
                scenario_id,
                corpus_hash=self._settings.corpus_hash,
                session_cap_yuan=self._settings.record_session_cap_yuan,
            )
        return self._recorder

    def _next_call_index(self, ctx: CallContext) -> int:
        key = (ctx.scenario_id, ctx.node_id, ctx.attempt)
        idx = self._call_counter.get(key, 0)
        self._call_counter[key] = idx + 1
        return idx

    def _price_for(self, model: str) -> ModelPrice:
        try:
            return self._prices[model]
        except KeyError as exc:
            raise ArgusError(f"价目表缺少模型 {model}（MODEL_PRICES）") from exc

    def _estimate_yuan(self, model: str, est_input: int, est_output: int) -> Decimal:
        price = self._price_for(model)
        return (
            Decimal(est_input) * price.input_per_1k + Decimal(est_output) * price.output_per_1k
        ) / 1000

    def _cost_from_usage(self, model: str, usage: LLMUsage) -> Decimal:
        price = self._price_for(model)
        return (
            Decimal(usage.input_tokens) * price.input_per_1k
            + Decimal(usage.output_tokens) * price.output_per_1k
        ) / 1000

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._settings.bailian_api_key.get_secret_value()}"}

    async def _request_with_backoff(self, path: str, body: Mapping[str, Any]) -> httpx.Response:
        """429 指数退避（0.5/1/2/4/8s 无抖动）+ 5xx 单次重试【实现细则】。"""
        url = f"{self._settings.llm_base_url}{path}"
        retried_5xx = False
        resp: httpx.Response | None = None
        for delay in _BACKOFF_DELAYS:
            if delay:
                await self._sleep(delay)
            resp = await self._http.post(
                url,
                json=dict(body),
                headers=self._auth_headers(),
                timeout=self._settings.llm_timeout_s,
            )
            if resp.status_code == 429:
                continue  # 供应商拥塞：只退让，不重建网关栈（03 §3.5）
            if resp.status_code >= 500 and not retried_5xx:
                retried_5xx = True
                continue
            break
        assert resp is not None  # _BACKOFF_DELAYS 非空
        if resp.status_code == 429:
            raise LLMCallError(429, congestion=True, detail="退避耗尽仍 429")
        if resp.status_code < 200 or resp.status_code >= 300:
            raise LLMCallError(resp.status_code, detail=resp.text[:200])
        return resp

    async def complete(
        self,
        *,
        tier: ModelTier,
        messages: Sequence[Message],
        ctx: CallContext,
        max_tokens: int = 1024,
        temperature: float | None = None,
    ) -> LLMResult:
        model = self.model_for(tier)
        call_index = self._next_call_index(ctx)
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": max_tokens,
        }
        if temperature is not None:
            body["temperature"] = temperature
        digest = request_digest(body)
        if self._settings.llm_mode == "replay":
            return self._replay_entry(ctx, call_index, digest)
        est_input = sum(len(m.content) for m in messages) // 2  # 【实现细则】中文≈2字符/token
        for g in self._gates:
            g.precheck(self._estimate_yuan(model, est_input, max_tokens))  # 超限不发请求
        resp = await self._request_with_backoff("/chat/completions", body)
        data = resp.json()
        usage_raw = data.get("usage") or {}
        usage = LLMUsage(
            input_tokens=int(usage_raw.get("prompt_tokens", 0)),
            output_tokens=int(usage_raw.get("completion_tokens", 0)),
            cached_tokens=int(
                (usage_raw.get("prompt_tokens_details") or {}).get("cached_tokens", 0)
            ),
        )
        cost = self._cost_from_usage(model, usage)
        result = LLMResult(
            text=data["choices"][0]["message"]["content"],
            model=str(data.get("model", model)),
            request_id=data.get("id"),
            usage=usage,
            cost_yuan=cost,
        )
        for g in self._gates:
            g.settle(cost)
        self._log.bind(task_id=ctx.task_id, role=ctx.role).info(
            "llm_call",
            request_id=result.request_id,
            model=result.model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cost_yuan=str(cost),
            call_index=call_index,
        )
        if self._settings.llm_mode == "record":
            from argus.core.cassette import CassetteKey

            self._recorder_for(ctx.scenario_id).record(
                CassetteKey(node_id=ctx.node_id, attempt=ctx.attempt, call_index=call_index),
                endpoint="chat",
                request_digest=digest,
                model=result.model,
                usage={
                    "input": usage.input_tokens,
                    "output": usage.output_tokens,
                    "cached": usage.cached_tokens,
                },
                cost_yuan=cost,
                response_text=result.text,
                request_id=result.request_id,
            )
        return result

    async def stream(
        self,
        *,
        tier: ModelTier,
        messages: Sequence[Message],
        ctx: CallContext,
        max_tokens: int = 1024,
        temperature: float | None = None,
    ) -> LLMStream:
        if self._settings.llm_mode != "live":
            raise ArgusError(
                "stream 暂仅支持 live：流式 cassette 化待 M1 消费方出现时定义【实现细则】"
            )
        model = self.model_for(tier)
        self._next_call_index(ctx)
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": max_tokens,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if temperature is not None:
            body["temperature"] = temperature
        est_input = sum(len(m.content) for m in messages) // 2
        for g in self._gates:
            g.precheck(self._estimate_yuan(model, est_input, max_tokens))
        request = self._http.build_request(
            "POST",
            f"{self._settings.llm_base_url}/chat/completions",
            json=body,
            headers=self._auth_headers(),
            timeout=self._settings.llm_timeout_s,
        )
        response = await self._http.send(request, stream=True)
        if response.status_code != 200:
            await response.aclose()
            raise LLMCallError(response.status_code, detail="stream 建立失败")
        return LLMStream(
            response=response,
            model=model,
            est_input_tokens=est_input,
            gates=tuple(self._gates),
            cost_fn=lambda u: self._cost_from_usage(model, u),
        )

    async def embed(self, texts: Sequence[str], ctx: CallContext) -> list[list[float]]:
        model = self._settings.embed_model
        out: list[list[float]] = []
        step = self._settings.embed_batch_size
        for start in range(0, len(texts), step):
            batch = list(texts[start : start + step])
            call_index = self._next_call_index(ctx)
            body = {"model": model, "input": batch}
            digest = request_digest(body)
            if self._settings.llm_mode == "replay":
                out.extend(self._replay_vectors(ctx, call_index, digest))
                continue
            est_input = sum(len(t) for t in batch) // 2
            for g in self._gates:
                g.precheck(self._estimate_yuan(model, est_input, 0))
            resp = await self._request_with_backoff("/embeddings", body)
            data = resp.json()
            usage_raw = data.get("usage") or {}
            usage = LLMUsage(input_tokens=int(usage_raw.get("prompt_tokens", 0)), output_tokens=0)
            cost = self._cost_from_usage(model, usage)
            for item in data["data"]:
                vec = [float(x) for x in item["embedding"]]
                if len(vec) != 1024:
                    raise ArgusError(f"embedding 维度异常：{len(vec)} ≠ 1024（07 §6）")
                out.append(vec)
            for g in self._gates:
                g.settle(cost)
            self._log.info(
                "llm_embed",
                model=model,
                batch=len(batch),
                input_tokens=usage.input_tokens,
                cost_yuan=str(cost),
            )
            if self._settings.llm_mode == "record":
                from argus.core.cassette import CassetteKey

                self._recorder_for(ctx.scenario_id).record(
                    CassetteKey(node_id=ctx.node_id, attempt=ctx.attempt, call_index=call_index),
                    endpoint="embeddings",
                    request_digest=digest,
                    model=model,
                    usage={"input": usage.input_tokens, "output": 0, "cached": 0},
                    cost_yuan=cost,
                    response_vectors=out[-len(batch) :],
                )
        return out

    async def aclose(self) -> None:
        if self._recorder is not None and self._recorder.has_entries:
            self._recorder.finalize()
        await self._http.aclose()
