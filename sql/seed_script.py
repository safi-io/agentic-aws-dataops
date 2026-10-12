import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SQL_FILE = PROJECT_ROOT / "sql" / "create_incidents.sql"

def main():
    target_db = os.environ["POSTGRES_DB"]
    host = os.environ["POSTGRES_HOST"]
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    print(f"Connecting to '{target_db}' to initialize schema...")
    sql_script = SQL_FILE.read_text(encoding="utf-8")

    with psycopg.connect(
        host=host,
        port=port,
        dbname=target_db,
        user=user,
        password=password,
        sslmode="require",
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_script)
    print("Database schema initialized successfully.")


if __name__ == "__main__":
    main()