"""Tool: profile_nulls - Profiles null counts, percentages, and threshold violations."""

from typing import Any, Dict, Optional
import pandas as pd
import os

DEFAULT_NULL_THRESHOLD = 0.15  # 15% maximum allowable missing rate


def profile_nulls(
    file_path: str,
    threshold: Optional[float] = None
) -> Dict[str, Any]:
    """Calculates column-level null rates and flags threshold violations."""
    if threshold is None:
        threshold = DEFAULT_NULL_THRESHOLD

    if not os.path.exists(file_path):
        return {
            "status": "failed",
            "error": f"File not found: {file_path}",
            "null_profile_valid": False,
        }

    try:
        df = pd.read_csv(file_path)
        total_rows = len(df)
        null_counts = df.isnull().sum().to_dict()
        null_percentages = {
            col: (count / total_rows) if total_rows > 0 else 0.0
            for col, count in null_counts.items()
        }

        violations = {
            col: pct
            for col, pct in null_percentages.items()
            if pct > threshold
        }

        has_violations = len(violations) > 0

        return {
            "status": "success",
            "null_profile_valid": not has_violations,
            "total_rows": total_rows,
            "threshold": threshold,
            "null_counts": null_counts,
            "null_percentages": {k: round(v, 4) for k, v in null_percentages.items()},
            "threshold_violations": {k: round(v, 4) for k, v in violations.items()},
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "null_profile_valid": False,
        }

