from decimal import Decimal
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from argus.core.types import ModelTier


class ArgusSettings(BaseSettings):
    """全项目配置收口（02 §6.3）：ARGUS_ 前缀、.env 加载、启动校验失败即拒绝启动。"""

    model_config = SettingsConfigDict(
        env_prefix="ARGUS_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    env: Literal["dev", "demo", "eval"] = "dev"
    pg_dsn: str = "postgresql+asyncpg://argus:argus@127.0.0.1:5432/argus"
    bailian_api_key: SecretStr = SecretStr("")
    llm_mode: Literal["replay", "record", "live"] = "replay"
    llm_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    corpus_hash: str | None = None
    worker_concurrency: int = 8
    budget_default_yuan: Decimal = Decimal("3.0")
    mcp_endpoints: dict[str, str] = Field(default_factory=dict)
    mcp_caller: str | None = None
    model_fast: str = "qwen-flash"
    model_heavy: str = "qwen-plus"
    model_escalate: str = "qwen-max"
    embed_model: str = "text-embedding-v4"
    record_session_cap_yuan: Decimal = Decimal(5)
    llm_timeout_s: float = 120.0
    embed_batch_size: int = 10

    @model_validator(mode="after")
    def _validate_startup(self) -> ArgusSettings:
        if self.env == "dev" and self.llm_mode != "replay":
            raise ValueError("dev 环境强制 replay（02 §6.3）：录制/live 请显式 ARGUS_ENV=demo")
        if self.llm_mode != "replay" and not self.bailian_api_key.get_secret_value():
            raise ValueError("record/live 模式必须提供 ARGUS_BAILIAN_API_KEY")
        if self.env == "eval" and (self.corpus_hash is None or self.budget_default_yuan <= 0):
            raise ValueError("eval 环境必须钉死 corpus_hash 且 budget_default_yuan > 0（02 §6.3）")
        if self.corpus_hash is not None and not self.corpus_hash.startswith("sha256:"):
            raise ValueError("corpus_hash 必须以 'sha256:' 开头")
        return self

    def model_for(self, tier: ModelTier) -> str:
        """档位映射（07 §6）：FLASH→model_fast，PLUS→model_heavy，MAX→model_escalate。"""
        mapping = {
            ModelTier.FLASH: self.model_fast,
            ModelTier.PLUS: self.model_heavy,
            ModelTier.MAX: self.model_escalate,
        }
        return mapping[tier]


@lru_cache
def get_settings() -> ArgusSettings:
    """进程级配置单例；测试不走它，直接 ArgusSettings(_env_file=None, **overrides) 构造。"""
    return ArgusSettings()
