-- Incident Tracking Table Schema

CREATE TABLE IF NOT EXISTS dataops_incidents (
    incident_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    dagster_run_id TEXT,
    source_uri TEXT,
    incident_type TEXT NOT NULL,
    severity TEXT DEFAULT 'medium',
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

CREATE INDEX IF NOT EXISTS idx_dataops_incidents_status ON dataops_incidents (status);
CREATE INDEX IF NOT EXISTS idx_dataops_incidents_pipeline_created ON dataops_incidents (pipeline_name, created_at DESC);

CREATE OR REPLACE FUNCTION update_dataops_incident_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_dataops_incident_timestamp ON dataops_incidents;
CREATE TRIGGER trg_update_dataops_incident_timestamp
BEFORE UPDATE ON dataops_incidents
FOR EACH ROW
EXECUTE FUNCTION update_dataops_incident_timestamp();

