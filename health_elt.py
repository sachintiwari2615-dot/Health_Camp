"""
health_elt.py
ETL Pipeline: CSV -> Transform -> SQLite

Steps:
  1. EXTRACT - read the project CSV with pandas
  2. TRANSFORM - clean, validate, engineer features, predict risk via ML model
  3. LOAD - bulk-insert into the `patients` table

Run once:
    python health_elt.py
"""

import os
import sys
import re
import numpy as np
import pandas as pd
import joblib

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(__file__))

import db
from generate_data import assign_risk_level, MODEL_PATH, ENCODER_PATH, resolve_csv_path

CSV_PATH = resolve_csv_path()

# Helpers

VALID_GENDERS = {"Male", "Female", "Other"}
VALID_BLOOD_GROUPS = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}
EMAIL_RE = re.compile(r"^[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}$")


def validate_row(row: pd.Series) -> list[str]:
    """Return a list of validation error strings (empty = row is valid)."""
    errors = []

    if pd.isna(row["Name"]) or str(row["Name"]).strip() == "":
        errors.append("Name is empty")

    if not (0 < int(row["Age"]) <= 120):
        errors.append(f"Invalid age: {row['Age']}")

    if str(row["Gender"]) not in VALID_GENDERS:
        errors.append(f"Invalid gender: {row['Gender']}")

    if str(row["Blood_Group"]) not in VALID_BLOOD_GROUPS:
        errors.append(f"Invalid blood group: {row['Blood_Group']}")

    if not EMAIL_RE.match(str(row["Email"])):
        errors.append(f"Invalid email: {row['Email']}")

    return errors


def clean_phone(phone) -> str:
    """Strip spaces/dashes and ensure 10-digit string."""
    return re.sub(r"\D", "", str(phone))[-10:]


# E - Extract

def extract(path: str) -> pd.DataFrame:
    print(f"[EXTRACT] Reading {path} ...")
    df = pd.read_csv(path)
    print(f"[EXTRACT] Raw rows: {len(df)}")
    return df


# T - Transform

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean & enrich the dataframe.
    Returns (clean_df, rejected_df).
    """
    print("[TRANSFORM] Starting ...")

    # --- Basic cleaning ---
    df = df.copy()
    df.columns = df.columns.str.strip()

    # Strip whitespace from string columns
    for col in df.select_dtypes("object").columns:
        df[col] = df[col].str.strip()

    # Normalise Gender capitalisation
    df["Gender"] = df["Gender"].str.capitalize()

    # Fill missing Existing_Condition with "None"
    df["Existing_Condition"] = df["Existing_Condition"].fillna("None")

    # Clean phone
    df["Phone"] = df["Phone"].apply(clean_phone)

    # Drop duplicate patient IDs (keep first)
    before = len(df)
    df = df.drop_duplicates(subset=["Patient_ID"], keep="first")
    print(f"[TRANSFORM] Removed {before - len(df)} duplicate Patient_ID rows")

    # Drop duplicate emails
    before = len(df)
    df = df.drop_duplicates(subset=["Email"], keep="first")
    print(f"[TRANSFORM] Removed {before - len(df)} duplicate Email rows")

    # --- Validation ---
    mask_valid = df.apply(lambda r: len(validate_row(r)) == 0, axis=1)
    rejected = df[~mask_valid].copy()
    clean = df[mask_valid].copy()
    print(f"[TRANSFORM] Valid rows: {len(clean)} | Rejected: {len(rejected)}")

    if len(rejected):
        print("[TRANSFORM] Sample rejected rows:")
        print(rejected.head(3)[["Patient_ID", "Name", "Email"]].to_string(index=False))

    # --- ML Risk Prediction ---
    print("[TRANSFORM] Predicting risk levels ...")
    _predict_risk_column(clean)

    print("[TRANSFORM] Done.")
    return clean, rejected


def _predict_risk_column(df: pd.DataFrame):
    """Add Risk_Level column using saved ML model (falls back to rule-based)."""
    try:
        pipeline = joblib.load(MODEL_PATH)
        le = joblib.load(ENCODER_PATH)

        features = df[["Age", "Gender", "Blood_Group", "Existing_Condition"]].copy()
        encoded = pipeline.predict(features)
        df["Risk_Level"] = le.inverse_transform(encoded)
        print("[TRANSFORM] Risk level predicted via ML model.")
    except FileNotFoundError:
        print("[TRANSFORM] WARNING: ML model not found - using rule-based fallback.")
        print("[TRANSFORM] Run python generate_data.py first to train the model.")
        df["Risk_Level"] = df.apply(assign_risk_level, axis=1)


# L - Load

def load(df: pd.DataFrame):
    """Insert transformed rows into the local app database."""
    print(f"[LOAD] Inserting {len(df)} patients ...")

    records = []
    for _, row in df.iterrows():
        records.append({
            "patient_id":          str(row["Patient_ID"]),
            "name":                str(row["Name"]),
            "age":                 int(row["Age"]),
            "gender":              str(row["Gender"]),
            "phone":               str(row["Phone"]),
            "email":               str(row["Email"]).lower(),
            "city":                str(row["City"]),
            "blood_group":         str(row["Blood_Group"]),
            "existing_condition":  None if row["Existing_Condition"] == "None" else str(row["Existing_Condition"]),
            "risk_level":          str(row["Risk_Level"]),
        })

    db.bulk_insert_patients(records)
    total = db.get_patient_count()
    print(f"[LOAD] Done. Total patients in DB: {total}")


# Pipeline runner

def run_elt():
    print("=" * 55)
    print(" Health Camp ELT Pipeline")
    print("=" * 55)

    # Bootstrap schema
    print("[INIT] Creating tables if not exist ...")
    db.create_tables()

    # Run pipeline
    raw_df = extract(CSV_PATH)
    clean_df, rejected_df = transform(raw_df)
    load(clean_df)

    # Save rejected rows for review
    if len(rejected_df):
        reject_path = os.path.join(os.path.dirname(__file__), "rejected_rows.csv")
        rejected_df.to_csv(reject_path, index=False)
        print(f"[ELT] Rejected rows saved to {reject_path}")

    print("=" * 55)
    print(" ELT Complete")
    print("=" * 55)


if __name__ == "__main__":
    run_elt()
