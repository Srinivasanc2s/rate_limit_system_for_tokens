"""
Reads new rows from the two append-only MySQL tables (query_log, request_log)
and writes them as one NDJSON file per batch into the Databricks Volume.
Run manually:  python export_to_volume.py
"""

import io
import json
import os
import time
from pathlib import Path

import mysql.connector
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv(override=True)

VOLUME_ROOT = "/Volumes/rate_limit_system/bronze/landing"
STATE_FILE = Path(__file__).with_name(".export_state.json")
BATCH_SIZE = 10_000

# table name -> its auto-increment primary key column
APPEND_TABLES = {
    "query_log": "q_id",
    "request_log": "log_id",
}


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {t: 0 for t in APPEND_TABLES}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_db():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "rate_limit"),
    )


def clean_value(value):
    # some MySQL driver versions return JSON columns as bytes
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return value


def rows_to_ndjson(rows: list[dict]) -> bytes:
    lines = []
    for row in rows:
        clean_row = {k: clean_value(v) for k, v in row.items()}
        lines.append(json.dumps(clean_row, default=str))  # default=str handles dates
    return ("\n".join(lines) + "\n").encode("utf-8")


def export_one_table(cursor, client, table, key_column, watermark):
    cursor.execute(
        f"SELECT * FROM {table} WHERE {key_column} > %s ORDER BY {key_column} LIMIT %s",
        (watermark, BATCH_SIZE),
    )
    rows = cursor.fetchall()

    if not rows:
        print(f"  {table}: nothing new (watermark={watermark})")
        return watermark

    first_id = rows[0][key_column]
    last_id = rows[-1][key_column]

    file_path = f"{VOLUME_ROOT}/{table}/{table}_{first_id:012d}_{last_id:012d}.json"
    client.files.upload(file_path, io.BytesIO(rows_to_ndjson(rows)), overwrite=True)

    print(f"  {table}: {len(rows)} rows -> {file_path}")
    return last_id


def main():
    state = load_state()
    client = WorkspaceClient()

    db = get_db()
    try:
        cursor = db.cursor(dictionary=True)
        print(f"[{time.strftime('%H:%M:%S')}] export starting")
        for table, key_column in APPEND_TABLES.items():
            new_watermark = export_one_table(cursor, client, table, key_column, state.get(table, 0))
            state[table] = new_watermark
        save_state(state)
    finally:
        db.close()


if __name__ == "__main__":
    main()
