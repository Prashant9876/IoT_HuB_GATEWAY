"""SQLite storage for THC, WLD, EMD, LSD and NSD gateway telemetry.

Call initialize_database() once from main.py before starting collector threads.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from database.schema import SCHEMAS

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "gateway.db"
IST = ZoneInfo("Asia/Kolkata")

Tables =[]

def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def initialize_database() -> None:
    """Create every table and add newly introduced missing columns."""
    with get_connection() as conn:
        for table, columns in SCHEMAS.items():
            definitions = ", ".join(["id INTEGER PRIMARY KEY AUTOINCREMENT", *[f'"{name}" {kind}' for name, kind in columns.items()]])
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({definitions})')

            present = {row["name"] for row in conn.execute(f'PRAGMA table_info("{table}")')}
            for column, data_type in columns.items():
                if column not in present:
                    # Existing rows require a nullable addition. New writes still provide every field.
                    conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {data_type.replace(" NOT NULL", "")}')

            conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table.lower()}_upload" ON "{table}" (uploaded, id)')


def table_status() -> dict[str, list[str]]:
    """Return present columns for every expected table."""
    result: dict[str, list[str]] = {}
    with get_connection() as conn:
        for table in SCHEMAS:
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            result[table] = [r["name"] for r in conn.execute(f'PRAGMA table_info("{table}")')] if exists else []
    return result


def list_available_tables() -> list[str]:
    """List every user-created table currently present in gateway.db."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    return [row["name"] for row in rows]


def _timestamps() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return now.isoformat(), now.astimezone(IST).isoformat()


def _write(table: str, values: dict) -> int:
    utc_timestamp, ist_timestamp = _timestamps()
    payload = {**values, "utc_timestamp": utc_timestamp, "ist_timestamp": ist_timestamp}
    columns = list(payload)
    placeholders = ", ".join("?" for _ in columns)
    with get_connection() as conn:
        cur = conn.execute(
            f'INSERT INTO "{table}" ({", ".join(chr(34) + key + chr(34) for key in columns)}) VALUES ({placeholders})',
            [payload[key] for key in columns],
        )
        return cur.lastrowid


def _read(table: str, *, uploaded: bool | None = None, limit: int = 100) -> list[dict]:
    sql = f'SELECT * FROM "{table}"'
    args: list[object] = []
    if uploaded is not None:
        sql += " WHERE uploaded=?"
        args.append(int(uploaded))
    sql += " ORDER BY id ASC LIMIT ?"
    args.append(limit)
    with get_connection() as conn:
        return [dict(row) for row in conn.execute(sql, args).fetchall()]


def write_thc(etemp: float, humidity: float, co2: float, farm_id: int, rack_id: int | None, shelf_id: int | None, device_id: str) -> int:
    return _write("THC", locals())


def write_wld(waterlevel: float, farm_id: int, rack_id: int | None, shelf_id: int | None, device_id: str) -> int:
    return _write("WLD", locals())


def write_emd(kwh: float, farm_id: int, rack_id: int | None, shelf_id: int | None, device_id: str) -> int:
    return _write("EMD", locals())


def write_lsd(ppfd: float, farm_id: int, rack_id: int | None, shelf_id: int | None, device_id: str) -> int:
    return _write("LSD", locals())


def write_nsd(ph_sensor: float, ec_sensor: float, water_temperature_sensor: float, farm_id: int, rack_id: int | None, shelf_id: int | None, device_id: str) -> int:
    return _write("NSD", locals())


def read_thc(**kwargs): return _read("THC", **kwargs)
def read_wld(**kwargs): return _read("WLD", **kwargs)
def read_emd(**kwargs): return _read("EMD", **kwargs)
def read_lsd(**kwargs): return _read("LSD", **kwargs)
def read_nsd(**kwargs): return _read("NSD", **kwargs)


def mark_uploaded(table: str, record_ids: list[int]) -> None:
    """Call only after MQTT broker acknowledges QoS 1 delivery."""
    if table not in SCHEMAS or not record_ids:
        return
    placeholders = ", ".join("?" for _ in record_ids)
    with get_connection() as conn:
        conn.execute(f'UPDATE "{table}" SET uploaded=1 WHERE id IN ({placeholders})', record_ids)




