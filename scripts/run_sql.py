"""Helper: apply a SQL file to the database via psycopg2.

Usage (from project root):
    python scripts/run_sql.py sql/006_flood_events.sql
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_connection


def run_file(path: str) -> None:
    sql = open(path).read()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    conn = get_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    msg = str(e).splitlines()[0]
                    if "already exists" in msg:
                        print(f"  [skip - already exists]: {msg}")
                    else:
                        print(f"ERROR: {msg}", file=sys.stderr)
                        sys.exit(1)
        print(f"Applied: {path}")
    finally:
        conn.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        run_file(f)
