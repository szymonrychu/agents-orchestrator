-- T-117 per-project chat rooms between hive leaders. Writer of project_posts: the api (the
-- agent post route inserts, fills recipients in the same transaction before commit); the api
-- sweeper is the only deleter (retention). messages.project_post_id is set by the api in the
-- fan-out transaction. sender_session_id has no foreign key on purpose: a post outlives its
-- session; messages.project_post_id has none either, posts are swept separately.
CREATE TABLE IF NOT EXISTS project_posts (
    id bigserial PRIMARY KEY,
    project text NOT NULL REFERENCES projects (slug),
    sender_kind text NOT NULL CHECK (sender_kind IN ('leader', 'superleader')),
    sender_session_id uuid NOT NULL,
    sender_slug text NOT NULL,
    text text NOT NULL,
    reply_to bigint REFERENCES project_posts (id) ON DELETE SET NULL,
    addressed text[] NOT NULL DEFAULT '{}',
    recipients text[] NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS project_posts_project_id ON project_posts (project, id DESC);
CREATE INDEX IF NOT EXISTS project_posts_created_at ON project_posts (created_at);

ALTER TABLE messages ADD COLUMN IF NOT EXISTS project_post_id bigint;
