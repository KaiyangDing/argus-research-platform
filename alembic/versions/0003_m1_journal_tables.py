"""m1 journal tables

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

# DDL 全文 = M1 册【实现细则】（02 待补录，见速查表 B.2/附录 Z-8）
_NODE_STEPS = """
-- EFFECTFUL 步骤日志（03 §4.3：先写意图行再执行，完成后写完成标记）
CREATE TABLE node_steps (
    node_id       UUID NOT NULL REFERENCES plan_nodes(id),
    attempt       INT  NOT NULL,                -- 哪个 attempt 写的意图（审计；续跑按 node 维度）
    step_no       INT  NOT NULL,
    step_key      TEXT NOT NULL,                -- 幂等键（03 §4.3：凭 step_key 安全重试）
    status        TEXT NOT NULL DEFAULT 'INTENT' CHECK (status IN ('INTENT','DONE')),
    result_digest TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, step_no),
    UNIQUE (node_id, step_key)
);
"""

_REPLAN_SIGNALS = """
-- 重规划信号（03 §5.1；状态机 open|handled|dismissed 见速查表 C.4；M1 只写不读）
CREATE TABLE replan_signals (
    id          BIGSERIAL PRIMARY KEY,
    task_id     UUID NOT NULL REFERENCES research_tasks(id),   -- 实现细则：M2 按任务聚合用
    node_id     UUID REFERENCES plan_nodes(id),
    kind        TEXT NOT NULL CHECK (kind IN
                  ('node_replan','evidence_conflict','coverage_gap','verifier_negative')),
    severity    INT  NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','handled','dismissed')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at  TIMESTAMPTZ
);
"""


def upgrade() -> None:
    op.execute(_NODE_STEPS)
    op.execute(_REPLAN_SIGNALS)
    op.execute(
        "CREATE INDEX idx_replan_signals_open ON replan_signals (task_id) WHERE status = 'open';"
    )


def downgrade() -> None:
    # 两表互不引用，先后均可；索引随表 drop（revision 链保证先于 0002 的表退场）
    op.execute("DROP TABLE replan_signals;")
    op.execute("DROP TABLE node_steps;")
