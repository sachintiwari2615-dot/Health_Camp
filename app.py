"""
Flask web application for the Health Camp Registration Portal.

The main UI is served from templates/health_camp_registration_portal.html and
is backed by JSON APIs for camps, registrations, and dashboard stats.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import date

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for

sys.path.insert(0, os.path.dirname(__file__))

import db
from generate_data import assign_risk_level, predict_risk

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24).hex())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
CONDITIONS = ["None", "Diabetes", "Heart Disease", "Hypertension", "Thyroid", "Asthma", "Allergy"]
CITIES = ["Delhi", "Mumbai", "Chennai", "Kolkata", "Bangalore", "Hyderabad", "Pune", "Lucknow", "Patna", "Jaipur", "Varanasi", "Surat", "Indore", "Bhopal", "Nagpur", "Bhubaneswar", "Cuttack"]
PHONE_RE = re.compile(r"\D")


with app.app_context():
    db.create_tables()
    db.seed_default_camps()
    logger.info("Database ready at %s", db.DB_PATH)


@app.context_processor
def inject_globals():
    return {"today": date.today().isoformat()}


def _request_data():
    if request.is_json:
        return request.get_json(force=True) or {}
    return request.form.to_dict()


def _clean_phone(phone: str) -> str:
    return PHONE_RE.sub("", str(phone or ""))[-10:]


def _predict_level(age: int, gender: str, blood_group: str, existing_condition: str | None) -> str:
    try:
        return predict_risk(
            age=age,
            gender=gender,
            blood_group=blood_group,
            existing_condition=existing_condition or "None",
        )
    except Exception as exc:
        logger.warning("ML prediction failed (%s), using fallback.", exc)
        return assign_risk_level(
            {
                "Age": age,
                "Existing_Condition": existing_condition,
            }
        )


def _validate_registration(data: dict) -> list[str]:
    errors = []
    if not data["name"]:
        errors.append("Name is required.")
    if data["age"] <= 0 or data["age"] > 120:
        errors.append("Age must be between 1 and 120.")
    if data["gender"] not in {"Male", "Female", "Other"}:
        errors.append("Gender is required.")
    if len(data["phone"]) != 10:
        errors.append("Phone must be exactly 10 digits.")
    if "@" not in data["email"]:
        errors.append("Valid email is required.")
    if data["blood_group"] not in BLOOD_GROUPS:
        errors.append("Select a valid blood group.")
    if not data["city"]:
        errors.append("City is required.")
    return errors


def _build_registration_payload(data: dict) -> tuple[dict, list[str]]:
    first_name = str(data.get("first_name", "")).strip()
    last_name = str(data.get("last_name", "")).strip()
    name = str(data.get("name", "")).strip() or " ".join(part for part in [first_name, last_name] if part).strip()

    try:
        age = int(str(data.get("age", "")).strip())
    except ValueError:
        age = -1

    phone = _clean_phone(data.get("phone", ""))
    email = str(data.get("email", "")).strip().lower()
    gender = str(data.get("gender", "")).strip()
    city = str(data.get("city", "")).strip()
    blood_group = str(data.get("blood_group", "")).strip()
    existing_condition = str(data.get("existing_condition", "None")).strip() or "None"
    camp_id = data.get("camp_id")

    normalized = {
        "name": name,
        "age": age,
        "gender": gender,
        "phone": phone,
        "email": email,
        "city": city,
        "blood_group": blood_group,
        "existing_condition": None if existing_condition == "None" else existing_condition,
        "camp_id": int(camp_id) if str(camp_id).strip().isdigit() else None,
    }
    return normalized, _validate_registration(normalized)


def _serialize_camps() -> list[dict]:
    serialized = []
    for camp in db.get_all_camps():
        registered = int(camp["registered"])
        capacity = int(camp["capacity"])
        if registered >= capacity:
            status = "full"
        elif camp["camp_date"] > date.today().isoformat():
            status = "open"
        else:
            status = "soon"
        serialized.append(
            {
                "id": camp["id"],
                "name": camp["camp_name"],
                "tagLabel": camp["camp_name"].split()[0],
                "date": camp["camp_date"],
                "location": camp["location"],
                "capacity": capacity,
                "reg": registered,
                "status": status,
            }
        )
    return serialized


@app.route("/")
def dashboard():
    return render_template(
        "health_camp_registration_portal.html",
        blood_groups=BLOOD_GROUPS,
        conditions=CONDITIONS,
        cities=CITIES,
    )


@app.route("/patients")
def patient_list():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = max(int(request.args.get("per_page", 20)), 1)
    offset = (page - 1) * per_page
    return jsonify(
        {
            "patients": db.get_all_patients(limit=per_page, offset=offset),
            "page": page,
            "per_page": per_page,
            "total": db.get_patient_count(),
        }
    )


@app.route("/patients/<patient_id>")
def patient_detail(patient_id: str):
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"error": f"Patient {patient_id} not found."}), 404
    return jsonify(patient)


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return redirect(url_for("dashboard"))
    return api_register_portal()


@app.route("/camps", methods=["GET", "POST"])
def camps():
    if request.method == "GET":
        return jsonify({"camps": _serialize_camps()})

    data = _request_data()
    camp_name = str(data.get("camp_name", "")).strip()
    location = str(data.get("location", "")).strip()
    camp_date = str(data.get("camp_date", "")).strip()
    try:
        capacity = int(str(data.get("capacity", "100")).strip())
    except ValueError:
        capacity = 100

    if not camp_name or not location or not camp_date:
        return jsonify({"error": "All camp fields are required."}), 400

    camp_id = db.insert_camp(
        {
            "camp_name": camp_name,
            "location": location,
            "camp_date": camp_date,
            "capacity": max(capacity, 1),
        }
    )
    return jsonify({"message": "Camp created successfully.", "camp_id": camp_id}), 201


@app.route("/camps/<int:camp_id>/register", methods=["POST"])
def register_to_camp(camp_id: int):
    data = _request_data()
    patient_id = str(data.get("patient_id", "")).strip()
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"error": "Patient not found."}), 404

    success = db.register_patient_for_camp(patient["id"], camp_id)
    if not success:
        return jsonify({"error": "Patient is already registered for this camp."}), 409

    return jsonify({"message": f"{patient['name']} registered successfully."})


@app.route("/camps/<int:camp_id>")
def camp_detail(camp_id: int):
    camp = db.get_camp_by_id(camp_id)
    if not camp:
        return jsonify({"error": "Camp not found."}), 404
    return jsonify({"camp": camp, "registrations": db.get_registrations_for_camp(camp_id)})


@app.route("/api/portal-data")
def api_portal_data():
    camps = _serialize_camps()
    participants = db.get_recent_registrations(limit=100)
    total_registered = sum(int(camp["reg"]) for camp in camps)
    return jsonify(
        {
            "stats": {
                "active_camps": len(camps),
                "total_registered": total_registered,
                "available_spots": sum(max(camp["capacity"] - camp["reg"], 0) for camp in camps),
                "specialists": 18,
                "fill_rate": db.get_fill_rate(),
                "upcoming_7d": db.get_upcoming_camp_count(7),
            },
            "camps": camps,
            "participants": [
                {
                    "patient_id": row["patient_id"],
                    "name": row["name"],
                    "age": row["age"],
                    "camp": row["camp"],
                    "date": row["registered_date"],
                }
                for row in participants
            ],
        }
    )


@app.route("/api/register-portal", methods=["POST"])
def api_register_portal():
    raw_data = _request_data()
    data, errors = _build_registration_payload(raw_data)
    if errors:
        return jsonify({"errors": errors}), 400

    if data["camp_id"] is None:
        return jsonify({"errors": ["Please select a camp."]}), 400

    camp = db.get_camp_by_id(data["camp_id"])
    if not camp:
        return jsonify({"errors": ["Selected camp was not found."]}), 404

    if int(camp["registered"]) >= int(camp["capacity"]):
        return jsonify({"errors": ["This camp is already full."]}), 409

    patient_id_num = f"P{db.get_patient_count() + 1:04d}"
    risk_level = _predict_level(
        age=data["age"],
        gender=data["gender"],
        blood_group=data["blood_group"],
        existing_condition=data["existing_condition"],
    )

    patient = {
        "patient_id": patient_id_num,
        "name": data["name"],
        "age": data["age"],
        "gender": data["gender"],
        "phone": data["phone"],
        "email": data["email"],
        "city": data["city"],
        "blood_group": data["blood_group"],
        "existing_condition": data["existing_condition"],
        "risk_level": risk_level,
    }

    new_id = db.insert_patient(patient)
    if new_id is None:
        return jsonify({"errors": ["A patient with this email or ID already exists."]}), 409

    db.register_patient_for_camp(new_id, data["camp_id"])
    return jsonify(
        {
            "message": "Registration successful.",
            "patient_id": patient_id_num,
            "risk_level": risk_level,
        }
    ), 201


@app.route("/api/stats")
def api_stats():
    return jsonify(
        {
            "total_patients": db.get_patient_count(),
            "risk_dist": db.get_risk_distribution(),
            "city_dist": db.get_city_distribution(),
            "condition_dist": db.get_condition_distribution(),
        }
    )


@app.route("/api/predict-risk", methods=["POST"])
def api_predict_risk():
    data = request.get_json(force=True)
    try:
        risk = _predict_level(
            age=int(data["age"]),
            gender=data["gender"],
            blood_group=data["blood_group"],
            existing_condition=data.get("existing_condition", "None"),
        )
        return jsonify({"risk_level": risk})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "true").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
