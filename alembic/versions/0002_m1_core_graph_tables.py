"""m1 core graph tables

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

# DDL 逐字取速查表 B.1（= 02 §3.2 镜像，Z-4 并集已回填）；注释按 M0 惯例截短
_RESEARCH_TASKS = """
CREATE TABLE research_tasks (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title             TEXT NOT NULL,
    objective         TEXT NOT NULL,              -- 研究目标原文
    status            TEXT NOT NULL DEFAULT 'SUBMITTED'
        CHECK (status IN ('SUBMITTED','PLANNING','AWAITING_APPROVAL','EXECUTING',
                          'DONE','DONE_DEGRADED','FAILED','CANCELLED')),
    plan_version      INT  NOT NULL DEFAULT 0,    -- 每次图手术 +1；审批乐观校验用
    corpus_hash       TEXT NOT NULL,              -- 本任务钉死的证据快照版本
    budget_tokens_cap BIGINT NOT NULL,            -- 根预算上限（token）
    budget_yuan_cap   NUMERIC(10,2) NOT NULL,     -- 根预算上限（人民币）
    idempotency_key   TEXT UNIQUE,                -- 提交幂等
    requested_by      TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
"""

_PLAN_NODES = """
CREATE TABLE plan_nodes (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id            UUID NOT NULL REFERENCES research_tasks(id),
    plan_version_added INT  NOT NULL,             -- 哪个计划版本引入（图手术审计）
    node_type          TEXT NOT NULL,             -- plan/research/analyze/verify/synthesize
    role               TEXT NOT NULL,             -- planner/researcher/analyst/verifier/synthesizer
    spec               JSONB NOT NULL,            -- NodeSpec：简报+输入引用+输出契约；
                                                  -- 含 replan_on_failure，不设实列（Z-26）
    -- 状态机
    status             TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','READY','RUNNING','DONE',
                          'FAILED','CANCELLED','NEEDS_REPLAN')),
    attempt            INT NOT NULL DEFAULT 0,
    max_attempts       INT NOT NULL DEFAULT 2,
    purity             TEXT NOT NULL DEFAULT 'pure'
        CHECK (purity IN ('pure','effectful')),   -- effectful 只在步骤边界取消/重试（03 §4）
    -- 调度与预算阻塞（03 §2.2 增补列，02 DDL ∪ 03 并集收编，Z-4）
    priority           INT NOT NULL DEFAULT 0,    -- 领取排序（ORDER BY priority DESC, ready_at）
    ready_at           TIMESTAMPTZ,               -- 进入 READY 时刻（T1 写、T11 清）
    blocked_reason     TEXT,                      -- NULL | 'budget'（03 §6.5）
    -- 取消意图不是状态，是列（03 §7.1）
    cancel_requested_at TIMESTAMPTZ,
    cancel_reason      TEXT,
    -- 租约（崩溃判活；心跳续约，reaper 收回过期租约）
    lease_owner        TEXT,
    lease_expires_at   TIMESTAMPTZ,
    -- 检查点引用：输出工件即检查点（DONE 必非空，I5）
    checkpoint_artifact_id UUID,                  -- REFERENCES artifacts(id)，迁移期后补 FK
    failure_class      TEXT,                      -- 失败分类（03 §9；Z-4 增补列）
    error              JSONB,                     -- 最近一次失败现场；03 行文 last_error 即本列
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ
);
"""

_PLAN_EDGES = """
CREATE TABLE plan_edges (
    task_id            UUID NOT NULL REFERENCES research_tasks(id),
    from_node          UUID NOT NULL REFERENCES plan_nodes(id),
    to_node            UUID NOT NULL REFERENCES plan_nodes(id),
    plan_version_added INT  NOT NULL,
    PRIMARY KEY (from_node, to_node),
    CHECK (from_node <> to_node)
);
"""

_ARTIFACTS = """
CREATE TABLE artifacts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id         UUID NOT NULL REFERENCES research_tasks(id),
    producer_node   UUID REFERENCES plan_nodes(id),
    kind            TEXT NOT NULL
        CHECK (kind IN ('plan_snapshot','research_note','analysis_table',
                        'verification_verdict','report_draft','report_final')),
    schema_name     TEXT NOT NULL,               -- Pydantic 契约名
    schema_version  INT  NOT NULL DEFAULT 1,
    payload         JSONB NOT NULL,              -- 原文级结构化内容（full 档）
    headline        TEXT NOT NULL,               -- 一句话摘要（headline 档）
    digest          TEXT,                        -- 段落摘要（digest 档：下游默认档位）
    refs            JSONB NOT NULL DEFAULT '[]', -- [{kind:'evidence'|'artifact', id, granularity}]
    topic_keys      TEXT[] NOT NULL DEFAULT '{}',-- 冲突检测主题键（03 §8.5）
    supersedes      UUID REFERENCES artifacts(id), -- 版本链：新指旧，旧行永不 UPDATE
    partial         BOOLEAN NOT NULL DEFAULT false, -- anytime 降级产物标记
    token_count     INT,                         -- payload 折算 token
    content_hash    TEXT NOT NULL,               -- 去重与幂等
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_DONE_GUARD_FN = """
CREATE FUNCTION plan_nodes_done_guard() RETURNS trigger AS $$
BEGIN
  IF OLD.status = 'DONE' AND NEW.status IS DISTINCT FROM 'DONE' THEN
    RAISE EXCEPTION 'plan_nodes: DONE is terminal (03-I5)';
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
"""

_DONE_GUARD_TRG = """
CREATE TRIGGER trg_plan_nodes_done_guard
  BEFORE UPDATE OF status ON plan_nodes
  FOR EACH ROW EXECUTE FUNCTION plan_nodes_done_guard();
"""


def upgrade() -> None:
    op.execute(_RESEARCH_TASKS)
    op.execute(_PLAN_NODES)
    op.execute(
        "CREATE INDEX idx_nodes_ready ON plan_nodes (task_id, created_at) WHERE status = 'READY';"
    )
    op.execute(
        "CREATE INDEX idx_nodes_lease ON plan_nodes (lease_expires_at) WHERE status = 'RUNNING';"
    )
    op.execute(_PLAN_EDGES)
    op.execute("CREATE INDEX idx_edges_task ON plan_edges (task_id);")
    op.execute(_ARTIFACTS)
    op.execute("CREATE INDEX idx_artifacts_task ON artifacts (task_id, kind);")
    op.execute("CREATE INDEX idx_artifacts_refs ON artifacts USING GIN (refs jsonb_path_ops);")
    op.execute(
        "ALTER TABLE plan_nodes ADD CONSTRAINT fk_nodes_checkpoint "
        "FOREIGN KEY (checkpoint_artifact_id) REFERENCES artifacts(id);"
    )
    op.execute(_DONE_GUARD_FN)
    op.execute(_DONE_GUARD_TRG)


def downgrade() -> None:
    # 逆序（M1 册 M1.2）：触发器→函数→FK→artifacts→plan_edges→部分索引→plan_nodes→research_tasks
    op.execute("DROP TRIGGER trg_plan_nodes_done_guard ON plan_nodes;")
    op.execute("DROP FUNCTION plan_nodes_done_guard;")
    op.execute("ALTER TABLE plan_nodes DROP CONSTRAINT fk_nodes_checkpoint;")
    op.execute("DROP TABLE artifacts;")
    op.execute("DROP TABLE plan_edges;")
    op.execute("DROP INDEX idx_nodes_ready;")
    op.execute("DROP INDEX idx_nodes_lease;")
    op.execute("DROP TABLE plan_nodes;")
    op.execute("DROP TABLE research_tasks;")
