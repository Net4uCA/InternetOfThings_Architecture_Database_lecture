from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
import json
from src.virtualization.digital_replica.dr_factory import DRFactory

led_api = Blueprint("led_api", __name__, url_prefix="/api/led")


def register_led_blueprint(app):
    """Register LED API blueprint with Flask app"""
    app.register_blueprint(led_api)


@led_api.route("/", methods=["POST"])
def create_led():
    """Create a new LED Digital Replica"""
    try:
        data = request.get_json()
        required_fields = ["name", "location"]
        if not all(field in data for field in required_fields):
            return jsonify({"error": "Missing required fields"}), 400

        initial_data = {
            "profile": {
                "name": data["name"],
                "location": data["location"],
                "description": data.get("description", ""),
            },
            "metadata": {"status": "active", "last_state_change": datetime.utcnow()},
            "data": {
                "state": "off",
                "brightness": 0,
                "measurements": [],
                "controlled_by": "system",
            },
        }

        # Use DRFactory to create LED
        dr_factory = DRFactory("src/virtualization/templates/led.yaml")
        led = dr_factory.create_dr("led", initial_data)

        # Save to database
        led_id = current_app.config["DB_SERVICE"].save_dr("led", led)
        return (
            jsonify(
                {
                    "status": "success",
                    "message": "LED created successfully",
                    "led_id": led_id,
                }
            ),
            201,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@led_api.route("/<led_id>", methods=["GET"])
def get_led(led_id):
    """Get LED details"""
    try:
        led = current_app.config["DB_SERVICE"].get_dr("led", led_id)
        if not led:
            return jsonify({"error": "LED not found"}), 404
        return jsonify(led), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@led_api.route("/", methods=["GET"])
def list_leds():
    """List all LEDs with optional filtering"""
    try:
        filters = {}
        if request.args.get("status"):
            filters["metadata.status"] = request.args.get("status")
        if request.args.get("state"):
            filters["data.state"] = request.args.get("state")

        leds = current_app.config["DB_SERVICE"].query_drs("led", filters)
        return jsonify({"leds": leds}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@led_api.route("/<led_id>/toggle", methods=["POST"])
def toggle_led(led_id):
    """Toggle LED state between on and off"""
    try:
        # Get current LED state
        led = current_app.config["DB_SERVICE"].get_dr("led", led_id)
        if not led:
            return jsonify({"error": "LED not found"}), 404

        # Get controller information
        data = request.get_json() or {}
        controlled_by = data.get("controlled_by", "api")

        # Toggle state
        new_state = "off" if led["data"]["state"] == "on" else "on"

        # Create measurement for state change
        measurement = {
            "type": "state_change",
            "value": 1.0 if new_state == "on" else 0.0,
            "timestamp": datetime.utcnow(),
        }

        # Update LED data
        update_data = {
            "data": {
                "state": new_state,
                "controlled_by": controlled_by,
                "measurements": led["data"]["measurements"] + [measurement],
            },
            "metadata": {
                "updated_at": datetime.utcnow(),
                "last_state_change": datetime.utcnow(),
            },
        }

        # Update in database
        current_app.config["DB_SERVICE"].update_dr("led", led_id, update_data)

        # Publish state change to MQTT if handler exists
        if (
            hasattr(current_app, "mqtt_handler")
            and current_app.mqtt_handler.is_connected
        ):
            topic = f"led/{led_id}/state"
            payload = {"state": new_state, "timestamp": datetime.utcnow().isoformat()}
            current_app.mqtt_handler.client.publish(topic, json.dumps(payload))

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"LED state changed to {new_state}",
                    "current_state": new_state,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
