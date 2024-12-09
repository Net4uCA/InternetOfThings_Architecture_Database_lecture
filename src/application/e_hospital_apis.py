from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from src.virtualization.digital_replica.dr_factory import DRFactory
from bson import ObjectId

hospital_api = Blueprint("e_hospital_api", __name__, url_prefix="/api/hospital")


def register_e_hospital_blueprint(app):
    app.register_blueprint(hospital_api)


# Room management endpoints
@hospital_api.route("/rooms", methods=["POST"])
def create_room():
    """Create a new room DR"""
    try:
        data = request.get_json()
        dr_factory = DRFactory("src/virtualization/templates/room.yaml")
        room = dr_factory.create_dr("room", data)
        room_id = current_app.config["DB_SERVICE"].save_dr("room", room)

        return (
            (
                jsonify(
                    {
                        "status": "success",
                        "message": "Room created successfully",
                        "room_id": room_id,
                    }
                )
            ),
            201,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@hospital_api.route("/rooms/<room_id>", methods=["GET"])
def get_room(room_id):
    """Get room details"""
    try:
        room = current_app.config["DB_SERVICE"].get_dr("room", room_id)
        if not room:
            return jsonify({"error": "Room not found"}), 404
        return jsonify(room), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Patient management endpoints
@hospital_api.route("/patients", methods=["POST"])
def create_patient():
    """Create a new patient DR"""
    try:
        data = request.get_json()
        dr_factory = DRFactory("src/virtualization/templates/patient.yaml")
        patient = dr_factory.create_dr("patient", data)
        patient_id = current_app.config["DB_SERVICE"].save_dr("patient", patient)

        # If room is specified, update room's current_patients
        if "room_id" in data:
            room_id = data["metadata"]["room_id"]
            room = current_app.config["DB_SERVICE"].get_dr("room", room_id)

            if room:
                # Update room's current_patients list
                room_update = {
                    "data": {
                        "current_patients": room["data"].get("current_patients", [])
                        + [patient_id]
                    },
                    "metadata": {"updated_at": datetime.utcnow(), "status": "occupied"},
                }
                current_app.config["DB_SERVICE"].update_dr("room", room_id, room_update)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Patient created and room updated successfully",
                    "patient_id": patient_id,
                }
            ),
            201,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@hospital_api.route("/patients/<patient_id>", methods=["GET"])
def get_patient(patient_id):
    """Get patient details"""
    try:
        patient = current_app.config["DB_SERVICE"].get_dr("patient", patient_id)
        if not patient:
            return jsonify({"error": "Patient not found"}), 404
        return jsonify(patient), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Doctor management endpoints
@hospital_api.route("/doctors", methods=["POST"])
def create_doctor():
    """Create a new doctor DR"""
    try:
        data = request.get_json()
        dr_factory = DRFactory("src/virtualization/templates/doctor.yaml")
        doctor = dr_factory.create_dr("doctor", data)
        doctor_id = current_app.config["DB_SERVICE"].save_dr("doctor", doctor)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Doctor created successfully",
                    "doctor_id": doctor_id,
                }
            )
        ), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@hospital_api.route("/doctors/<doctor_id>", methods=["GET"])
def get_doctor(doctor_id):
    """Get doctor details"""
    try:
        doctor = current_app.config["DB_SERVICE"].get_dr("doctor", doctor_id)
        if not doctor:
            return jsonify({"error": "Doctor not found"}), 404
        return jsonify(doctor), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# RFID access management
@hospital_api.route("/rooms/<room_id>/access", methods=["POST"])
def record_room_access(room_id):
    """Record doctor's room access via RFID"""
    try:
        data = request.get_json()
        if not data.get("rfid_tag"):
            return jsonify({"error": "Missing RFID tag"}), 400

        # Find doctor by RFID
        doctors = current_app.config["DB_SERVICE"].query_drs(
            "doctor", {"profile.rfid_tag": data["rfid_tag"]}
        )

        if not doctors:
            return jsonify({"error": "Doctor not found for given RFID"}), 404

        doctor = doctors[0]
        doctor_id = doctor["_id"]
        access_timestamp = datetime.utcnow()

        # Record access in room's log
        room = current_app.config["DB_SERVICE"].get_dr("room", room_id)
        if not room:
            return jsonify({"error": "Room not found"}), 404

        access_log = {
            "doctor_id": doctor_id,
            "timestamp": access_timestamp,
            "access_type": data.get("access_type", "visit"),
        }

        # Update room's access logs
        room_update = {
            "data": {"access_logs": room["data"].get("access_logs", []) + [access_log]},
            "metadata": {"updated_at": access_timestamp},
        }
        current_app.config["DB_SERVICE"].update_dr("room", room_id, room_update)

        # Update doctor's access history
        doctor_access = {
            "room_id": room_id,
            "timestamp": access_timestamp,
            "access_type": data.get("access_type", "visit"),
        }

        # Get current access history or initialize empty list
        current_history = doctor.get("data", {}).get("room_access_history", [])
        doctor_update = {
            "data": {"room_access_history": current_history + [doctor_access]},
            "metadata": {"updated_at": access_timestamp},
        }
        current_app.config["DB_SERVICE"].update_dr("doctor", doctor_id, doctor_update)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Room access recorded in both room and doctor history",
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
