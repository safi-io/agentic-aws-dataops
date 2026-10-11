"""Tool: inspect_schema - Validates observed dataset schema against contract."""

from typing import Any, Dict, List, Optional
import pandas as pd
import os

DEFAULT_CONTRACT = {
    "expected_columns": ["order_id", "customer_id", "amount"],
    "required_columns": ["order_id", "customer_id", "amount"],
}


def inspect_schema(
    file_path: str,
    contract: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Inspects a CSV file against expected schema contracts.

    Returns structured diagnostics including missing, unexpected, and observed columns.
    """
    if contract is None:
        contract = DEFAULT_CONTRACT

    expected_cols = set(contract.get("expected_columns", []))
    required_cols = set(contract.get("required_columns", []))

    if not os.path.exists(file_path):
        return {
            "status": "failed",
            "error": f"File not found: {file_path}",
            "schema_valid": False,
        }

    try:
        # Read header and first few lines for inspection
        df = pd.read_csv(file_path, nrows=5)
        observed_cols = list(df.columns)
        observed_set = set(observed_cols)

        missing_cols = list(required_cols - observed_set)
        unexpected_cols = list(observed_set - expected_cols)
        matched_cols = list(observed_set.intersection(expected_cols))

        is_valid = len(missing_cols) == 0 and len(unexpected_cols) == 0

        return {
            "status": "success",
            "schema_valid": is_valid,
            "observed_columns": observed_cols,
            "expected_columns": list(expected_cols),
            "missing_columns": missing_cols,
            "unexpected_columns": unexpected_cols,
            "matched_columns": matched_cols,
            "inferred_dtypes": {col: str(df[col].dtype) for col in df.columns},
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "schema_valid": False,
        }

