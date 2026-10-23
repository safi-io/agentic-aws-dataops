CREATE TABLE IF NOT EXISTS dataops_incidents (
    incident_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    dagster_run_id TEXT,
    source_uri TEXT,
    incident_type TEXT NOT NULL,
    severity TEXT,
    error_summary TEXT,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    selected_action TEXT,
    decision_confidence DOUBLE PRECISION,
    action_result JSONB,
    incident_report TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dataops_incidents_status_created
    ON dataops_incidents (status, created_at);

CREATE INDEX IF NOT EXISTS idx_dataops_incidents_pipeline_updated
    ON dataops_incidents (pipeline_name, updated_at DESC);

CREATE OR REPLACE FUNCTION set_dataops_incidents_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dataops_incidents_updated_at ON dataops_incidents;

CREATE TRIGGER trg_dataops_incidents_updated_at
BEFORE UPDATE ON dataops_incidents
FOR EACH ROW
EXECUTE FUNCTION set_dataops_incidents_updated_at();

