"""Render cloud-only settings without printing credentials or copying local .env files."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote

import boto3
import bcrypt
from botocore.config import Config


def write_private(path: str, content: str) -> None:
    target = Path(path)
    target.write_text(content, encoding="utf-8")
    target.chmod(0o640)
    subprocess.run(["chown", "root:dataops", str(target)], check=True)


def main() -> None:
    region = os.environ["AWS_DEFAULT_REGION"]
    secrets = boto3.client(
        "secretsmanager", region_name=region,
        config=Config(retries={"mode": "standard", "total_max_attempts": 4}),
    )
    db = json.loads(secrets.get_secret_value(
        SecretId=os.environ["DATAOPS_DB_SECRET_ARN"],
    )["SecretString"])
    team = json.loads(secrets.get_secret_value(
        SecretId=os.environ["DATAOPS_TEAM_SECRET_ARN"],
    )["SecretString"])
    host = os.environ["DATAOPS_PUBLIC_HOST"]
    db_url = (
        f"postgresql+psycopg://{quote(db['username'], safe='')}:"
        f"{quote(db['password'], safe='')}@{os.environ['DATAOPS_RDS_HOST']}:5432/"
        "dataops?sslmode=require"
    )
    settings = {
        "DATABASE_URL": db_url,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
        "S3_BUCKET_NAME": os.environ["DATAOPS_BUCKET"],
        "S3_BRONZE_PREFIX": "bronze/",
        "S3_SILVER_PREFIX": "silver/shared-demo/",
        "S3_QUARANTINE_PREFIX": "quarantine/shared-demo/",
        "DECIDER_MODE": "cli",
        "DECIDER_TIMEOUT_SECONDS": "600",
        "STRANDS_DECIDER_MODEL_ID": "StrandsAgents/strands-decider-2B-hobson-v19",
        "BEDROCK_MODEL_ID": "amazon.nova-micro-v1:0",
        # Demo actions remain visible in the DB without emailing SNS subscribers.
        "ALERT_SNS_TOPIC_ARN": "",
        "DAGSTER_HOME": "/var/lib/dataops/dagster",
        "PYTHONPATH": "/opt/dataops/app",
        "PATH": "/opt/dataops/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HF_HOME": "/var/lib/dataops/huggingface",
        "TOKENIZERS_PARALLELISM": "false",
        "OMP_NUM_THREADS": "2",
        "DATAOPS_ENV_FILE": "/etc/dataops/application.env",
    }
    # Values are generated here and contain no quotes/newlines; JSON quoting also
    # works with both systemd EnvironmentFile and python-dotenv.
    env = "".join(f"{key}={json.dumps(value)}\n" for key, value in settings.items())
    write_private("/etc/dataops/runtime.env", env)
    write_private("/etc/dataops/application.env", env)
    Path("/var/lib/dataops/dagster/dagster.yaml").write_text(
        "telemetry:\n  enabled: false\n"
        "run_coordinator:\n  module: dagster.core.run_coordinator\n"
        "  class: QueuedRunCoordinator\n  config:\n    max_concurrent_runs: 1\n",
        encoding="utf-8",
    )
    password_hash = bcrypt.hashpw(
        team["password"].encode("utf-8"), bcrypt.gensalt(rounds=12),
    ).decode("ascii")
    caddy = f"""{host} {{
    basicauth {{
        {team["username"]} {password_hash}
    }}
    header {{
        Strict-Transport-Security "max-age=31536000"
        X-Content-Type-Options nosniff
        X-Frame-Options SAMEORIGIN
        Referrer-Policy same-origin
    }}
    redir /dagster /dagster/
    redir /dashboard /dashboard/
    handle /dagster/* {{
        reverse_proxy 127.0.0.1:3000
    }}
    handle /dashboard/* {{
        reverse_proxy 127.0.0.1:8501
    }}
    handle {{
        root * /opt/dataops/app/deploy/aws/site
        file_server
    }}
}}
"""
    Path("/etc/caddy/Caddyfile").write_text(caddy, encoding="utf-8")
    Path("/etc/caddy/Caddyfile").chmod(0o640)
    subprocess.run(["chown", "root:caddy", "/etc/caddy/Caddyfile"], check=True)
    subprocess.run(["caddy", "validate", "--config", "/etc/caddy/Caddyfile"], check=True)


if __name__ == "__main__":
    main()
