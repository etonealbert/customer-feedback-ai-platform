CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tenants (
    id uuid PRIMARY KEY, name text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash text PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
    active boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS feedback (
    id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
    idempotency_key varchar(128) NOT NULL, request_hash text NOT NULL,
    text text NOT NULL, source varchar(80) NOT NULL, metadata jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL CHECK (status IN ('pending','completed','failed')),
    model_version text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id,idempotency_key), UNIQUE (id,tenant_id)
);
CREATE INDEX IF NOT EXISTS feedback_tenant_created ON feedback(tenant_id,created_at DESC);
CREATE TABLE IF NOT EXISTS predictions (
    feedback_id uuid NOT NULL, tenant_id uuid NOT NULL, model_version text NOT NULL,
    sentiment text NOT NULL CHECK (sentiment IN ('positive','negative','neutral')),
    confidence double precision NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    topics text[] NOT NULL, embedding vector(128) NOT NULL, embedding_version text NOT NULL,
    inference_ms double precision NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (feedback_id,model_version),
    FOREIGN KEY (feedback_id,tenant_id) REFERENCES feedback(id,tenant_id)
);
CREATE INDEX IF NOT EXISTS predictions_tenant ON predictions(tenant_id,embedding_version);
CREATE TABLE IF NOT EXISTS processing_attempts (
    feedback_id uuid NOT NULL, tenant_id uuid NOT NULL, model_version text NOT NULL,
    attempts integer NOT NULL DEFAULT 0, last_error text,
    PRIMARY KEY (feedback_id,model_version),
    FOREIGN KEY (feedback_id,tenant_id) REFERENCES feedback(id,tenant_id)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id uuid PRIMARY KEY, topic text NOT NULL, partition_key text NOT NULL,
    payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS dead_letters (
    event_id uuid PRIMARY KEY, payload jsonb NOT NULL, error_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedback FORCE ROW LEVEL SECURITY;
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE predictions FORCE ROW LEVEL SECURITY;
ALTER TABLE processing_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE processing_attempts FORCE ROW LEVEL SECURITY;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename='feedback' AND policyname='tenant_isolation') THEN
        CREATE POLICY tenant_isolation ON feedback
        USING (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid)
        WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid);
        CREATE POLICY tenant_isolation ON predictions
        USING (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid)
        WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid);
        CREATE POLICY tenant_isolation ON processing_attempts
        USING (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid)
        WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id',true),'')::uuid);
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO feedback_app;
GRANT SELECT ON tenants,api_keys TO feedback_app;
GRANT SELECT,INSERT,UPDATE ON feedback,predictions,processing_attempts,outbox,dead_letters TO feedback_app;
INSERT INTO schema_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
