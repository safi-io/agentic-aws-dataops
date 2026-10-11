"""Tool: quarantine_batch - Isolates faulty data batches into a quarantine prefix."""

import os
import shutil
from typing import Any, Dict, Optional
from datetime import datetime, timezone


def quarantine_batch(
    source_path: str,
    quarantine_dir: Optional[str] = None,
    incident_id: Optional[str] = None
) -> Dict[str, Any]:
    """Copies the failing dataset to an isolated quarantine location.

    Preserves the original Bronze record while isolating the problematic data batch.
    """
    if not os.path.exists(source_path):
        return {
            "status": "failed",
            "error": f"Source file does not exist: {source_path}",
            "quarantined": False,
        }

    if quarantine_dir is None:
        quarantine_dir = os.path.join(os.path.dirname(source_path), "..", "quarantine")

    os.makedirs(quarantine_dir, exist_ok=True)

    base_name = os.path.basename(source_path)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix = f"{incident_id}_" if incident_id else ""
    target_filename = f"{prefix}{timestamp}_{base_name}"
    target_path = os.path.join(quarantine_dir, target_filename)

    try:
        shutil.copy2(source_path, target_path)
        file_size = os.path.getsize(target_path)

        return {
            "status": "success",
            "quarantined": True,
            "source_path": source_path,
            "quarantined_destination": target_path,
            "bytes_quarantined": file_size,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "confirmation": f"Successfully quarantined batch to {target_path}",
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "quarantined": False,
        }

