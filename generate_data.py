"""
generate_data.py
Trains a Random Forest classifier to predict patient RISK LEVEL
(Low / Medium / High) from: Age, Gender, Blood_Group, Existing_Condition.

Usage:
    python generate_data.py

Outputs:
    risk_model.pkl      - trained pipeline (encoders + model)
    label_encoder.pkl   - LabelEncoder for the target column

The model is then used by app.py to score new registrations in real-time.
"""

import os
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, accuracy_score

def resolve_csv_path() -> str:
    base_dir = os.path.dirname(__file__)
    candidates = [
        os.path.join(base_dir, "health_data.csv"),
        os.path.join(base_dir, "healthcare_dataset_1200.csv"),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


CSV_PATH = resolve_csv_path()
MODEL_PATH = os.path.join(os.path.dirname(__file__), "risk_model.pkl")
ENCODER_PATH = os.path.join(os.path.dirname(__file__), "label_encoder.pkl")


# Risk label generation (business rules)

HIGH_RISK_CONDITIONS = {"Heart Disease", "Diabetes", "Hypertension"}
MEDIUM_RISK_CONDITIONS = {"Thyroid", "Asthma", "Allergy"}


def assign_risk_level(row: pd.Series) -> str:
    """
    Rule-based risk labelling used to generate training targets.
    Priority order: High > Medium > Low
    """
    condition = row["Existing_Condition"]
    age = row["Age"]

    if pd.isna(condition):
        # No condition: age determines risk
        if age >= 60:
            return "Medium"
        return "Low"

    if condition in HIGH_RISK_CONDITIONS:
        return "High" if age >= 40 else "Medium"

    if condition in MEDIUM_RISK_CONDITIONS:
        return "Medium" if age >= 30 else "Low"

    return "Low"


# Load and prepare data

def load_and_prepare(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Fill missing condition as "None"
    df["Existing_Condition"] = df["Existing_Condition"].fillna("None")

    # Generate risk labels
    df["Risk_Level"] = df.apply(assign_risk_level, axis=1)

    return df


# Feature engineering

FEATURE_COLS = ["Age", "Gender", "Blood_Group", "Existing_Condition"]
TARGET_COL = "Risk_Level"

CATEGORICAL_COLS = ["Gender", "Blood_Group", "Existing_Condition"]
NUMERICAL_COLS = ["Age"]


def build_preprocessor():
    """OrdinalEncoder for categoricals (works well with tree models)."""
    cat_transformer = OrdinalEncoder(
        handle_unknown="use_encoded_value",
        unknown_value=-1,
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", NUMERICAL_COLS),
            ("cat", cat_transformer, CATEGORICAL_COLS),
        ]
    )
    return preprocessor


# Train

def train(df: pd.DataFrame):
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    # Encode target
    le = LabelEncoder()
    y_enc = le.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y_enc, test_size=0.2, random_state=42, stratify=y_enc
    )

    pipeline = Pipeline([
        ("preprocessor", build_preprocessor()),
        ("classifier", RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )),
    ])

    pipeline.fit(X_train, y_train)

    # Evaluation
    y_pred = pipeline.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"\nTest Accuracy : {acc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

    # 5-fold cross-validation
    cv_scores = cross_val_score(pipeline, X, y_enc, cv=5, scoring="accuracy")
    print(f"Cross-val Accuracy: {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")

    return pipeline, le


# Predict helper (used by app.py)

def predict_risk(age: int, gender: str, blood_group: str,
                 existing_condition: str) -> str:
    """
    Load the saved model and return a risk level string.
    existing_condition: pass "None" when there is no condition.
    """
    pipeline = joblib.load(MODEL_PATH)
    le: LabelEncoder = joblib.load(ENCODER_PATH)

    sample = pd.DataFrame([{
        "Age": int(age),
        "Gender": gender,
        "Blood_Group": blood_group,
        "Existing_Condition": existing_condition if existing_condition else "None",
    }])

    encoded = pipeline.predict(sample)
    return le.inverse_transform(encoded)[0]


# Main

if __name__ == "__main__":
    print("Loading data from:", CSV_PATH)
    df = load_and_prepare(CSV_PATH)

    print(f"   Rows: {len(df)} | Columns: {list(df.columns)}")
    print("\nRisk Level Distribution:")
    print(df["Risk_Level"].value_counts())

    print("\nTraining Random Forest model ...")
    pipeline, label_encoder = train(df)

    joblib.dump(pipeline, MODEL_PATH)
    joblib.dump(label_encoder, ENCODER_PATH)

    print(f"\nModel saved to {MODEL_PATH}")
    print(f"Encoder saved to {ENCODER_PATH}")

    # Quick smoke test
    test_risk = predict_risk(65, "Male", "A+", "Diabetes")
    print(f"\nSmoke test (65yr Male, A+, Diabetes) -> Risk: {test_risk}")

    test_risk2 = predict_risk(22, "Female", "O+", "None")
    print(f"Smoke test (22yr Female, O+, None) -> Risk: {test_risk2}")
