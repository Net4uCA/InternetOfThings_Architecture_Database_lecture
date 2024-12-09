# src/application/mqtt_handler.py
import paho.mqtt.client as mqtt
from flask import current_app
from datetime import datetime
import json
import logging
from threading import Thread, Event
import time

# Configurazione logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class HospitalMQTTHandler:
    def __init__(self, app):
        print("Initializing Hospital MQTT Handler")
        self.app = app
        self.client = mqtt.Client(client_id="hospital_handler", clean_session=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.client.on_subscribe = self._on_subscribe
        self._setup_mqtt()
        self.connected = False
        self.stopping = Event()
        self.reconnect_thread = None

        # Enable debugging in Paho
        self.client.enable_logger(logger)

    def _setup_mqtt(self):
        """Setup MQTT configuration"""
        config = self.app.config.get("MQTT_CONFIG", {})
        self.broker = config.get("broker", "broker.hivemq.com")
        self.port = config.get("port", 1883)

        # Topics to subscribe
        self.topics = [
            ("hospital/+/+/rfid", 0),  # floor/room/rfid
            (
                "hospital/patient/+/vitals/#",
                0,
            ),  # patient/id/vitals/[heart_rate|blood_pressure|temperature|oxygen]
        ]
        print(f"MQTT Setup - Broker: {self.broker}, Port: {self.port}")
        print(f"Topics: {self.topics}")

    def start(self):
        """Start MQTT client"""
        try:
            print("Starting MQTT handler...")
            self.client.loop_start()
            self._connect()
            self.reconnect_thread = Thread(target=self._reconnection_loop)
            self.reconnect_thread.daemon = True
            self.reconnect_thread.start()
            print("MQTT handler started")
        except Exception as e:
            print(f"Error starting MQTT handler: {str(e)}")
            raise

    def stop(self):
        """Stop MQTT client"""
        print("Stopping MQTT handler...")
        self.stopping.set()
        if self.reconnect_thread:
            self.reconnect_thread.join(timeout=1.0)
        self.client.loop_stop()
        if self.connected:
            self.client.disconnect()
        print("MQTT handler stopped")

    def _connect(self):
        """Connect to MQTT broker"""
        try:
            print(f"Connecting to {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port, keepalive=60)
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            self.connected = False

    def _reconnection_loop(self):
        """Handle reconnection"""
        while not self.stopping.is_set():
            if not self.connected:
                print("Attempting to reconnect...")
                try:
                    self._connect()
                except Exception as e:
                    print(f"Reconnection failed: {str(e)}")
            time.sleep(5)

    def _on_connect(self, client, userdata, flags, rc):
        """Connection callback"""
        print(f"MQTT Connection callback - rc: {rc}")
        if rc == 0:
            print("Connected to MQTT broker")
            self.connected = True
            # Subscribe to all topics
            for topic, qos in self.topics:
                result = client.subscribe(topic, qos)
                print(f"Subscription to {topic} result: {result}")
        else:
            print(f"Connection failed with code {rc}")
            self.connected = False

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Subscription callback"""
        print(f"Subscribed to topics - MID: {mid}, QOS: {granted_qos}")

    def _on_disconnect(self, client, userdata, rc):
        """Disconnection callback"""
        self.connected = False
        if rc != 0:
            print(f"Unexpected disconnection: {rc}")

    def _on_message(self, client, userdata, msg):
        """Message callback"""
        print(f"Received message on {msg.topic}: {msg.payload}")
        try:
            topic_parts = msg.topic.split("/")

            # Handle RFID messages
            if "rfid" in topic_parts:
                self._handle_rfid_message(topic_parts, msg.payload)
            # Handle patient vital signs
            elif "vitals" in topic_parts:
                self._handle_vitals_message(topic_parts, msg.payload)

        except Exception as e:
            print(f"Error processing message: {str(e)}")

    def _find_room(self, floor: str, room_number: str):
        """Find room by floor and number"""
        try:
            rooms = current_app.config["DB_SERVICE"].query_drs(
                "room",
                {"profile.floor": int(floor), "profile.room_number": room_number},
            )
            return rooms[0] if rooms else None
        except Exception as e:
            print(f"Error finding room: {str(e)}")
            return None

    def _process_rfid_access(self, room_id: str, rfid_tag: str):
        """Process RFID access"""
        try:
            # Find doctor
            doctors = current_app.config["DB_SERVICE"].query_drs(
                "doctor", {"profile.rfid_tag": rfid_tag}
            )

            if not doctors:
                print(f"Doctor not found for RFID: {rfid_tag}")
                return

            doctor = doctors[0]
            doctor_id = doctor["_id"]
            access_timestamp = datetime.utcnow()

            print(f"Found doctor {doctor_id} with RFID {rfid_tag}")

            # Get room
            room = current_app.config["DB_SERVICE"].get_dr("room", room_id)
            if not room:
                print(f"Room not found: {room_id}")
                return

            # Update room access logs
            access_log = {
                "doctor_id": doctor_id,
                "timestamp": access_timestamp,
                "access_type": "visit",
            }

            current_logs = room.get("data", {}).get("access_logs", [])
            room_update = {
                "data": {"access_logs": current_logs + [access_log]},
                "metadata": {"updated_at": access_timestamp},
            }
            current_app.config["DB_SERVICE"].update_dr("room", room_id, room_update)

            # Update doctor access history
            doctor_access = {
                "room_id": room_id,
                "timestamp": access_timestamp,
                "access_type": "visit",
            }

            current_history = doctor.get("data", {}).get("room_access_history", [])
            doctor_update = {
                "data": {"room_access_history": current_history + [doctor_access]},
                "metadata": {"updated_at": access_timestamp},
            }
            current_app.config["DB_SERVICE"].update_dr(
                "doctor", doctor_id, doctor_update
            )

            print(f"Access recorded - Room: {room_id}, Doctor: {doctor_id}")

        except Exception as e:
            print(f"Error processing RFID access: {str(e)}")

    @property
    def is_connected(self):
        """Connection status"""
        return self.connected

    def _handle_vitals_message(self, topic_parts, payload):
        """Handle vital signs messages"""
        try:
            # Topic format: hospital/patient/[patient_id]/vitals/[vital_type]
            if len(topic_parts) != 5:
                print(f"Invalid vitals topic format: {'/'.join(topic_parts)}")
                return

            _, _, patient_id, _, vital_type = topic_parts
            print(
                f"Processing vital sign - Patient ID from topic: {patient_id}, Type: {vital_type}"
            )

            try:
                value = float(payload.decode())
            except ValueError:
                print(f"Invalid vital sign value: {payload}")
                return

            with self.app.app_context():
                patients = current_app.config["DB_SERVICE"].query_drs(
                    "patient", {"profile.patient_id": patient_id}
                )

                if not patients:
                    print(f"Patient not found with patient_id: {patient_id}")
                    return

                patient = patients[0]
                real_id = patient["_id"]
                print(f"Found patient with _id: {real_id}")

                # Create measurement
                measurement = {
                    "device_id": f"{vital_type}_sensor",
                    "measure_type": vital_type,
                    "value": value,
                    "timestamp": datetime.utcnow(),
                }

                # Update patient measurements
                current_measurements = patient.get("data", {}).get("measurements", [])
                patient_update = {
                    "data": {"measurements": current_measurements + [measurement]},
                    "metadata": {"updated_at": datetime.utcnow()},
                }

                current_app.config["DB_SERVICE"].update_dr(
                    "patient", real_id, patient_update
                )
                print(
                    f"Vital sign recorded - Patient: {patient_id} (real_id: {real_id}), Type: {vital_type}, Value: {value}"
                )

        except Exception as e:
            print(f"Error processing vital sign: {str(e)}")

    def _handle_rfid_message(self, topic_parts, payload):
        """Handle RFID messages"""
        try:
            # Topic format: hospital/[floor]/[room]/rfid
            if len(topic_parts) != 4:
                print(f"Invalid RFID topic format: {'/'.join(topic_parts)}")
                return

            _, floor, room_number, _ = topic_parts
            print(f"Processing RFID - Floor: {floor}, Room: {room_number}")

            with self.app.app_context():
                room = self._find_room(floor, room_number)
                if not room:
                    print(f"Room not found - Floor: {floor}, Room: {room_number}")
                    return

                try:
                    rfid_data = json.loads(payload.decode())
                    rfid_tag = rfid_data.get("rfid_tag")
                    if not rfid_tag:
                        print("Missing RFID tag in payload")
                        return

                    print(f"Processing RFID tag: {rfid_tag}")
                    self._process_rfid_access(room["_id"], rfid_tag)

                except json.JSONDecodeError:
                    print(f"Invalid JSON payload: {payload}")

        except Exception as e:
            print(f"Error processing RFID message: {str(e)}")
