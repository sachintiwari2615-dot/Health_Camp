"""
db.py - SQLite-backed data access layer for the Health Camp Registration Portal.

The original project was wired to PostgreSQL, but this repo ships without a
database server configuration. Using a local SQLite file keeps the project
portable and lets the Flask app run out of the box.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import date
from typing import Iterable

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(__file__), "health_camp.db"),
)


def _dicts(rows: Iterable[sqlite3.Row]) -> list[dict]:
    return [dict(row) for row in rows]


@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def create_tables():
    ddl = """
    CREATE TABLE IF NOT EXISTS patients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        gender TEXT NOT NULL,
        phone TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        city TEXT NOT NULL,
        blood_group TEXT NOT NULL,
        existing_condition TEXT,
        risk_level TEXT,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS camps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        camp_name TEXT NOT NULL,
        location TEXT NOT NULL,
        camp_date TEXT NOT NULL,
        capacity INTEGER NOT NULL DEFAULT 100,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patient_id INTEGER NOT NULL REFERENCES patients(id) ON DELETE CASCADE,
        camp_id INTEGER NOT NULL REFERENCES camps(id) ON DELETE CASCADE,
        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (patient_id, camp_id)
    );
    """
    with get_connection() as conn:
        conn.executescript(ddl)


def seed_default_camps():
    default_camps = [
        ("General Health Screening", "Community Hall, Bhubaneswar", "2026-04-14", 80),
        ("Eye Care & Vision Camp", "SDH Cuttack Road", "2026-04-19", 60),
        ("Dental Hygiene Drive", "Municipal School, Unit 4", "2026-04-21", 50),
        ("Women's Wellness Camp", "PHC Patia", "2026-04-26", 70),
        ("Fitness & Diabetes Check", "Sports Complex, Kalinga Nagar", "2026-05-02", 90),
    ]
    with get_connection() as conn:
        current = conn.execute("SELECT COUNT(*) FROM camps").fetchone()[0]
        if current:
            return
        conn.executemany(
            """
            INSERT INTO camps (camp_name, location, camp_date, capacity)
            VALUES (?, ?, ?, ?)
            """,
            default_camps,
        )


def insert_patient(patient: dict) -> int | None:
    sql = """
        INSERT INTO patients (
            patient_id, name, age, gender, phone, email, city,
            blood_group, existing_condition, risk_level
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = (
        patient["patient_id"],
        patient["name"],
        patient["age"],
        patient["gender"],
        patient["phone"],
        patient["email"],
        patient["city"],
        patient["blood_group"],
        patient["existing_condition"],
        patient["risk_level"],
    )
    with get_connection() as conn:
        try:
            cur = conn.execute(sql, values)
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            return None


def bulk_insert_patients(patients: list[dict]):
    sql = """
        INSERT OR IGNORE INTO patients (
            patient_id, name, age, gender, phone, email, city,
            blood_group, existing_condition, risk_level
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    rows = [
        (
            patient["patient_id"],
            patient["name"],
            patient["age"],
            patient["gender"],
            patient["phone"],
            patient["email"],
            patient["city"],
            patient["blood_group"],
            patient["existing_condition"],
            patient["risk_level"],
        )
        for patient in patients
    ]
    with get_connection() as conn:
        conn.executemany(sql, rows)


def get_all_patients(limit: int = 100, offset: int = 0) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, patient_id, name, age, gender, phone, email, city,
                   blood_group, existing_condition, risk_level, registered_at
            FROM patients
            ORDER BY registered_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    return _dicts(rows)


def get_patient_by_id(patient_id: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM patients WHERE patient_id = ?",
            (patient_id,),
        ).fetchone()
    return dict(row) if row else None


def get_patient_count() -> int:
    with get_connection() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0])


def get_risk_distribution() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(risk_level, 'Unknown') AS risk_level, COUNT(*) AS total
            FROM patients
            GROUP BY COALESCE(risk_level, 'Unknown')
            ORDER BY total DESC, risk_level
            """
        ).fetchall()
    return _dicts(rows)


def get_city_distribution() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT city, COUNT(*) AS total
            FROM patients
            GROUP BY city
            ORDER BY total DESC, city
            LIMIT 10
            """
        ).fetchall()
    return _dicts(rows)


def get_condition_distribution() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(existing_condition, 'None') AS condition, COUNT(*) AS total
            FROM patients
            GROUP BY COALESCE(existing_condition, 'None')
            ORDER BY total DESC, condition
            """
        ).fetchall()
    return _dicts(rows)


def insert_camp(camp: dict) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO camps (camp_name, location, camp_date, capacity)
            VALUES (?, ?, ?, ?)
            """,
            (
                camp["camp_name"],
                camp["location"],
                camp["camp_date"],
                camp["capacity"],
            ),
        )
        return int(cur.lastrowid)


def get_all_camps() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.camp_name, c.location, c.camp_date, c.capacity,
                   COUNT(r.id) AS registered
            FROM camps c
            LEFT JOIN registrations r ON r.camp_id = c.id
            GROUP BY c.id, c.camp_name, c.location, c.camp_date, c.capacity
            ORDER BY c.camp_date ASC, c.id ASC
            """
        ).fetchall()
    return _dicts(rows)


def get_camp_by_id(camp_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT c.id, c.camp_name, c.location, c.camp_date, c.capacity,
                   COUNT(r.id) AS registered
            FROM camps c
            LEFT JOIN registrations r ON r.camp_id = c.id
            WHERE c.id = ?
            GROUP BY c.id, c.camp_name, c.location, c.camp_date, c.capacity
            """,
            (camp_id,),
        ).fetchone()
    return dict(row) if row else None


def register_patient_for_camp(patient_db_id: int, camp_id: int) -> bool:
    with get_connection() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO registrations (patient_id, camp_id)
                VALUES (?, ?)
                """,
                (patient_db_id, camp_id),
            )
            return cur.rowcount > 0
        except sqlite3.IntegrityError:
            return False


def get_registrations_for_camp(camp_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.patient_id, p.name, p.age, p.gender, p.blood_group,
                   p.existing_condition, p.risk_level, r.registered_at
            FROM registrations r
            JOIN patients p ON p.id = r.patient_id
            WHERE r.camp_id = ?
            ORDER BY r.registered_at DESC, r.id DESC
            """,
            (camp_id,),
        ).fetchall()
    return _dicts(rows)


def get_recent_registrations(limit: int = 50) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT p.patient_id, p.name, p.age, c.camp_name AS camp,
                   DATE(r.registered_at) AS registered_date,
                   r.registered_at
            FROM registrations r
            JOIN patients p ON p.id = r.patient_id
            JOIN camps c ON c.id = r.camp_id
            ORDER BY r.registered_at DESC, r.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return _dicts(rows)


def get_fill_rate() -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(capacity), 0) AS total_capacity,
                   COALESCE((
                       SELECT COUNT(*)
                       FROM registrations
                   ), 0) AS total_registered
            FROM camps
            """
        ).fetchone()
    total_capacity = int(row["total_capacity"])
    total_registered = int(row["total_registered"])
    if total_capacity == 0:
        return 0
    return round((total_registered / total_capacity) * 100)


def get_upcoming_camp_count(days: int = 7) -> int:
    today = date.today().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM camps
            WHERE camp_date BETWEEN ? AND DATE(?, '+' || ? || ' day')
            """,
            (today, today, days),
        ).fetchone()
    return int(row[0])
