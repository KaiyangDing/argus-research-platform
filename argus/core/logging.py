import logging
import sys

import structlog


def configure_logging(*, level: str = "INFO") -> None:
    """structlog JSON 配置（02 §7.1）：merge_contextvars → add_log_level → ISO 时间戳 → JSON 到 stdout。"""
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s", force=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """绑定字段约定（02 §7.1）：task_id/node_id/attempt/plan_version/role 用 .bind() 挂上。"""
    if name is None:
        return structlog.stdlib.get_logger()
    return structlog.stdlib.get_logger(name)
