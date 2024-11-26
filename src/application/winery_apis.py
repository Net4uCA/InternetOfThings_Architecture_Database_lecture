from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from bson import ObjectId
from src.virtualization.digital_replica.dr_factory import DRFactory

# Create blueprint for winery-specific APIs
winery_api = Blueprint('winery_api', __name__, url_prefix='/api/winery')


@winery_api.route('/bottles', methods=['POST'])
def create_bottle():
    """Create a new bottle"""
    try:
        data = request.get_json()
        dr_factory = DRFactory('src/virtualization/templates/bottle.yaml')
        bottle = dr_factory.create_dr('bottle', data)

        # Save to database
        bottle_id = current_app.config['DB_SERVICE'].save_dr("bottle", bottle)

        return jsonify({
            "status": "success",
            "message": "Bottle created successfully",
            "bottle_id": bottle_id
        }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@winery_api.route('/bottles/<bottle_id>', methods=['GET'])
def get_bottle(bottle_id):
    """Get bottle details"""
    try:
        bottle = current_app.config['DB_SERVICE'].get_dr("bottle", bottle_id)
        if not bottle:
            return jsonify({"error": "Bottle not found"}), 404
        return jsonify(bottle), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/bottles', methods=['GET'])
def list_bottles():
    """List all bottles with optional filtering"""
    try:
        # Get query parameters
        filters = {}
        if request.args.get('status'):
            filters['data.status'] = request.args.get('status')

        bottles = current_app.config['DB_SERVICE'].query_drs("bottle", filters)
        return jsonify(bottles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms', methods=['POST'])
def create_room():
    """Create a new room"""
    try:
        data = request.get_json()
        dr_factory = DRFactory('src/virtualization/templates/room.yaml')
        room = dr_factory.create_dr('room', data)

        # Save to database
        room_id = current_app.config['DB_SERVICE'].save_dr("room", room)

        return jsonify({
            "status": "success",
            "message": "Room created successfully",
            "room_id": room_id
        }), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms/<room_id>', methods=['GET'])
def get_room(room_id):
    """Get room details"""
    try:
        room = current_app.config['DB_SERVICE'].get_dr("room", room_id)
        if not room:
            return jsonify({"error": "Room not found"}), 404
        return jsonify(room), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms/<room_id>', methods=['PUT'])
def update_room(room_id):
    """Update room information"""
    try:
        data = request.get_json()
        update_data = {}

        # Handle profile updates
        if "profile" in data:
            update_data["profile"] = data["profile"]

        # Handle data updates
        if "data" in data:
            update_data["data"] = data["data"]

        # Always update the updated_at timestamp
        update_data["metadata"] = {"updated_at": datetime.utcnow()}

        current_app.config['DB_SERVICE'].update_dr("room", room_id, update_data)
        return jsonify({"status": "success", "message": "Room updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms/<room_id>', methods=['DELETE'])
def delete_room(room_id):
    """Delete a room"""
    try:
        room = current_app.config['DB_SERVICE'].get_dr("room", room_id)
        if not room:
            return jsonify({"error": "Room not found"}), 404

        if room['data']['bottles'] or room['data']['devices']:
            return jsonify({
                "error": "Cannot delete room: remove all bottles and devices first"
            }), 400

        current_app.config['DB_SERVICE'].delete_dr("room", room_id)
        return jsonify({"status": "success", "message": "Room deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms', methods=['GET'])
def list_rooms():
    """List all rooms with optional filtering"""
    try:
        # Get query parameters for filtering
        filters = {}
        if request.args.get('status'):
            filters['data.status'] = request.args.get('status')
        if request.args.get('floor'):
            filters['profile.floor'] = int(request.args.get('floor'))

        rooms = current_app.config['DB_SERVICE'].query_drs("room", filters)
        return jsonify(rooms), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@winery_api.route('/rooms/<room_id>/measurements', methods=['POST'])
def add_room_measurement(room_id):
    """Add a new measurement to a room, with special handling for RFID readings"""
    try:
        data = request.get_json()
        if not data.get('measure_type') or 'value' not in data:
            return jsonify({"error": "Missing required measurement fields"}), 400

        # Get current room data
        room = current_app.config['DB_SERVICE'].get_dr("room", room_id)
        if not room:
            return jsonify({"error": "Room not found"}), 404

        # Inizializza i campi se non esistono
        if 'data' not in room:
            room['data'] = {}
        if 'bottles' not in room['data']:
            room['data']['bottles'] = []
        if 'measurements' not in room['data']:
            room['data']['measurements'] = []

        # Se è una lettura RFID
        if data['measure_type'] == 'rfid':
            rfid_tag = data['value']

            # Cerca la bottiglia con questo tag
            bottles = current_app.config['DB_SERVICE'].query_drs(
                "bottle",
                {"profile.rfid_tag": rfid_tag}
            )

            if not bottles:
                return jsonify({
                    "error": f"No bottle found with RFID tag: {rfid_tag}"
                }), 404

            bottle = bottles[0]
            bottle_id = bottle['_id']
            old_room_id = bottle['data'].get('room_id') if 'data' in bottle else None

            # Se la bottiglia era in un'altra stanza, rimuovila
            if old_room_id and old_room_id != room_id:
                old_room = current_app.config['DB_SERVICE'].get_dr("room", old_room_id)
                if old_room and 'data' in old_room and 'bottles' in old_room['data']:
                    if bottle_id in old_room['data']['bottles']:
                        old_room['data']['bottles'].remove(bottle_id)
                        old_room_update = {
                            "data": {"bottles": old_room['data']['bottles']},
                            "metadata": {"updated_at": datetime.utcnow()}
                        }
                        current_app.config['DB_SERVICE'].update_dr("room", old_room_id, old_room_update)

            # Aggiorna la bottiglia con la nuova stanza
            bottle_update = {
                "data": {"room_id": room_id},
                "metadata": {"updated_at": datetime.utcnow()}
            }
            current_app.config['DB_SERVICE'].update_dr("bottle", bottle_id, bottle_update)

            # Aggiungi la bottiglia alla nuova stanza se non c'è già
            if bottle_id not in room['data']['bottles']:
                room['data']['bottles'].append(bottle_id)

        # Registra comunque la misurazione
        measurement = {
            "measure_type": data['measure_type'],
            "value": data['value'],
            "timestamp": datetime.utcnow()
        }

        update_data = {
            "data": {
                "measurements": room['data']['measurements'] + [measurement],
                "bottles": room['data']['bottles']  # Include sempre la lista bottles aggiornata
            },
            "metadata": {
                "updated_at": datetime.utcnow()
            }
        }

        current_app.config['DB_SERVICE'].update_dr("room", room_id, update_data)

        return jsonify({
            "status": "success",
            "message": "Measurement processed successfully"
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



def register_winery_blueprint(app):
    """Register the winery blueprint with the Flask app"""
    app.register_blueprint(winery_api)