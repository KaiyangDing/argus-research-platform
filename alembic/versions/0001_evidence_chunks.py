"""evidence_chunks

Revision ID: 0001
Revises:
Create Date: <保留生成值>
"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_DDL = """
CREATE TABLE evidence_chunks (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    corpus_hash  TEXT NOT NULL,                  -- 语料快照版本（与任务钉死的一致才可被引用）
    source_id    TEXT NOT NULL,                  -- 快照内文档 id
    doc_title    TEXT NOT NULL,
    source_type  TEXT NOT NULL
        CHECK (source_type IN ('annual_report','official_notice','news','judgment')),
    source_tier  SMALLINT NOT NULL,              -- 来源分级：1=年报 2=官方公示 3=新闻（claim 佐证权重用）
    published_at DATE,
    locator      JSONB NOT NULL,                 -- {page, para_offset,...} 溯源页跳转坐标
    text         TEXT NOT NULL,
    text_hash    TEXT NOT NULL,
    embedding    vector(1024),                   -- 百炼 text-embedding-v4
    tsv          tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
    UNIQUE (corpus_hash, text_hash)
);
"""


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    op.execute(_DDL)
    op.execute(
        "CREATE INDEX idx_chunks_vec ON evidence_chunks USING hnsw (embedding vector_cosine_ops);"
    )
    op.execute("CREATE INDEX idx_chunks_tsv ON evidence_chunks USING GIN (tsv);")


def downgrade() -> None:
    op.execute("DROP INDEX idx_chunks_vec;")
    op.execute("DROP INDEX idx_chunks_tsv;")
    op.execute("DROP TABLE evidence_chunks;")
