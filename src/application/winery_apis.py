from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from src.virtualization.digital_replica.dr_factory import DRFactory
from bson import ObjectId

winery_api = Blueprint('winery_api', __name__,url_prefix = '/api/winery')

def register_winery_blueprint(app):
    app.register_blueprint(winery_api)


@winery_api.route("/bottles",methods=['POST'])
def create_bottle():
    try:
        data = request.get_json()
        dr_factory = DRFactory("src/virtualization/templates/bottle.yaml")
        bottle = dr_factory.create_dr('bottle',data)
        bottle_id = current_app.config["DB_SERVICE"].save_dr("bottle",bottle)
        return jsonify({"status":"success","message":"Bottle created successfully","bottle_id":bottle_id}), 201
    except Exception as e:
        return jsonify({"error":str(e)}),500
@winery_api.route("/bottles/<bottle_id>", methods=['GET'])
def get_bottle(bottle_id):
    "Get bottle details"
    try:
        bottle = current_app.config["DB_SERVICE"].get_dr("bottle",bottle_id)
        if not bottle:
            return jsonify({"error":"Bottle not found"}), 404
        return jsonify(bottle), 200
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/bottles",methods=['GET'])
def list_bottles():
    "Get all bottles with optional filtering"
    try:
        filters = {}
        if request.args.get('status'):
            filters["metadata.status"] = request.args.get('status')
        bottles = current_app.config["DB_SERVICE"].query_drs("bottle",filters)
        return jsonify({"bottles":bottles}), 200
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/rooms", methods=['POST'])
def create_room():
    try:
        data = request.get_json()
        dr_factory = DRFactory("src/virtualization/templates/room.yaml")
        room = dr_factory.create_dr('room',data)
        #Save to database
        room_id = current_app.config["DB_SERVICE"].save_dr("room",room)
        return jsonify({"status":"success","message":"Room created successfully","room_id":room_id}), 201
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/rooms/<room_id>", methods=['GET'])
def get_room(room_id):
    """Get room details"""
    try:
        room = current_app.config["DB_SERVICE"].get_dr("room",room_id)
        if not room:
            return jsonify({"error":"Room not found"}), 404
        return jsonify(room),200
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/rooms/<room_id>", methods=['PUT'])
def update_room(room_id):
    """Update room details"""
    try:
        data = request.get_json()
        update_data = {}

        #Handle profile updates
        if "profile" in data:
            update_data["profile"] = data["profile"]
        #Handle data updates
        if "data" in data:
            update_data["data"] = data["data"]

        #Always update the 'updated at' timestamp
        update_data["metadata"] = {"updated_at":datetime.utcnow()}

        current_app.config["DB_SERVICE"].update_dr("room",room_id,update_data)
        return jsonify({"status":"success","message":"Room updated successfully"}), 200
    except Exception as e:
        return jsonify({"error":str(e)}),500
@winery_api.route("/rooms/<room_id>", methods=['DELETE'])
def delete_room(room_id):
    """Delete a room"""
    try:
        room = current_app.config["DB_SERVICE"].get_dr("room",room_id)
        if not room:
            return jsonify({"error":"Room not found"}), 404
        if room['data']['bottles'] or room['data']['devices']:
            return jsonify({"error":"Room cannot be deleted, remove all bottles and devices first!"}), 400
        current_app.config["DB_SERVICE"].delete_dr("room",room_id)
        return jsonify({"status":"success","message":"Room deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/rooms", methods=['GET'])
def list_rooms():
    """List all rooms with optional filtering"""
    try:
        filters = {}
        if request.args.get('status'):
            filters["data.status"] = request.args.get('status')
        if request.args.get('floor'):
            filters["profile.floor"] = int(request.args.get('floor'))
        room = current_app.config["DB_SERVICE"].query_drs("room",filters)
        return jsonify({"rooms":room}), 200
    except Exception as e:
        return jsonify({"error":str(e)}),500

@winery_api.route("/rooms/<room_id>/measurements", methods=['POST'])
def add_room_measurements(room_id):
    """Add a new measurement to a room, with special handling for RFID readings"""
    try:
        data = request.get_json()
        if not data.get('measure_type') or 'value' not in data:
            return jsonify({"error":"Missing required measurement fields"}), 400
        #get current room data
        room = current_app.config["DB_SERVICE"].get_dr("room",room_id)
        if not room:
            return jsonify({"error":"Room not found"}), 404
        #initilize fields if they do not exist
        if 'data' not in room:
            room['data'] = {}
        if 'bottles' not in room['data']:
            room['data']['bottles'] = []
        # if the measure type is RFID
        if data['measure_type'] == 'rfid':
            rfid_tag=data['value']
            #Find the bottle with this RFID tag
            bottles = current_app.config["DB_SERVICE"].query_drs("bottle",{'profile.rfid_tag':rfid_tag})
            bottle = bottles[0] #take the first - TODO add a bottle check for univocity
            bottle_id = bottle['_id']
            old_room_id = bottle['data'].get('room_id') if 'data' in bottle else None
            # if the bottle was in another room, remove it from there
            if old_room_id and old_room_id != room_id:
                old_room = current_app["DB_SERVICE"].get_dr("room",old_room_id)
                if old_room and 'data' in old_room and 'bottles' in old_room['data']:
                    if bottle_id in old_room['data']['bottles']:
                        old_room['data']['bottles'].remove(bottle_id)
                        old_room_update = {
                            "data": {"bottles": old_room['data']['bottles']},
                            "metadata": {"updated_at": datetime.utcnow()}
                        }
                        current_app.config['DB_SERVICE'].update_dr("room", old_room_id, old_room_update)
            # Update the bottle with the new room
            bottle_update = {
                "data": {"room_id": room_id},
                "metadata": {"updated_at": datetime.utcnow()}
            }
            current_app.config['DB_SERVICE'].update_dr("bottle", bottle_id, bottle_update)
            # Add the bottle at new new room
            if bottle_id not in room['data']['bottles']:
                room['data']['bottles'].append(bottle_id)
        #We need to register the measurement
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
        return jsonify({"error":str(e)}),500


@winery_api.route('/temperature-prediction/<dt_id>', methods=['POST'])
def predict_bottle_temperature(dt_id):
    """
    Predict optimal room temperature for a bottle within a Digital Twin

    Expected JSON body:
    {
        "bottle_id": "string"  # ID of the bottle to analyze
    }
    """
    try:
        data = request.get_json()
        if not data or 'bottle_id' not in data:
            return jsonify({'error': 'bottle_id is required in request body'}), 400

        # Get DT instance
        dt = current_app.config['DT_FACTORY'].get_dt_instance(dt_id)
        if not dt:
            return jsonify({'error': 'Digital Twin not found'}), 404

        # Execute temperature prediction service
        try:
            prediction = dt.execute_service(
                'TemperaturePredictionService',
                bottle_id=data['bottle_id']
            )
            return jsonify(prediction), 200
        except ValueError as ve:
            return jsonify({'error': str(ve)}), 400
        except Exception as e:
            return jsonify({'error': f'Service execution failed: {str(e)}'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500