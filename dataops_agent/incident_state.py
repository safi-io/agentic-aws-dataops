from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class IncidentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    RESOLVED = "resolved"
    NEEDS_REVIEW = "needs_review"


class IncidentSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IncidentRecord(BaseModel):
    """Pydantic model representing a structured incident row."""
    incident_id: str
    pipeline_name: str
    dagster_run_id: Optional[str] = None
    source_uri: str
    incident_type: str
    severity: IncidentSeverity = IncidentSeverity.MEDIUM
    error_summary: Optional[str] = None
    evidence_json: Dict[str, Any] = Field(default_factory=dict)
    status: IncidentStatus = IncidentStatus.PENDING
    selected_action: Optional[str] = None
    decision_confidence: Optional[float] = None
    action_result: Optional[Dict[str, Any]] = None
    incident_report: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize model for database insertion or JSON serialization."""
        data = self.model_dump()
        data["severity"] = self.severity.value
        data["status"] = self.status.value
        data["created_at"] = self.created_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        return data

