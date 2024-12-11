from flask import current_app
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import logging
import time
from threading import Thread, Event

logger = logging.getLogger(__name__)


class LEDMQTTHandler:
    def __init__(self, app):
        self.app = app
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self._setup_mqtt()
        self.connected = False
        self.stopping = Event()
        self.reconnect_thread = None

    def _setup_mqtt(self):
        """Setup MQTT client with configuration from app"""
        config = self.app.config.get("MQTT_CONFIG", {})
        self.broker = config.get("broker", "broker.mqttdashboard.com")
        self.port = config.get("port", 1883)
        self.base_topic = "home/led/"  # Base topic per i LED

    def start(self):
        """Start MQTT client in non-blocking way"""
        try:
            self.client.loop_start()
            self._connect()
            self.reconnect_thread = Thread(target=self._reconnection_loop)
            self.reconnect_thread.daemon = True
            self.reconnect_thread.start()
            logger.info("MQTT handler started")
        except Exception as e:
            logger.error(f"Error starting MQTT handler: {e}")

    def stop(self):
        """Stop MQTT client"""
        self.stopping.set()
        if self.reconnect_thread:
            self.reconnect_thread.join(timeout=1.0)
        self.client.loop_stop()
        if self.connected:
            self.client.disconnect()
        logger.info("MQTT handler stopped")

    def _connect(self):
        """Attempt to connect to the broker"""
        try:
            self.client.connect(self.broker, self.port, 60)
            logger.info(f"Attempting connection to {self.broker}:{self.port}")
        except Exception as e:
            logger.error(f"Connection attempt failed: {e}")
            self.connected = False

    def _reconnection_loop(self):
        """Background thread that handles reconnection"""
        while not self.stopping.is_set():
            if not self.connected:
                logger.info("Attempting to reconnect...")
                try:
                    self._connect()
                except Exception as e:
                    logger.error(f"Reconnection attempt failed: {e}")
            time.sleep(5)

    def _on_connect(self, client, userdata, flags, rc):
        """Handle connection to broker"""
        if rc == 0:
            self.connected = True
            logger.info("Connected to MQTT broker")
        else:
            self.connected = False
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection from broker"""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker: {rc}")

    def _on_message(self, client, userdata, msg):
        """Handle incoming messages"""
        # For now we don't handle input messages
        pass

    def publish_led_state(self, led_id: str, state: str):
        """Publish LED state change"""
        if not self.connected:
            logger.error("Not connected to MQTT broker")
            return

        topic = f"{self.base_topic}{led_id}/state"
        payload = {"state": state, "timestamp": datetime.utcnow().isoformat()}

        try:
            self.client.publish(topic, json.dumps(payload))
            logger.info(f"Published state {state} for LED {led_id}")
        except Exception as e:
            logger.error(f"Error publishing LED state: {e}")

    def publish_led_brightness(self, led_id: str, brightness: int):
        """Publish LED brightness change"""
        if not self.connected:
            logger.error("Not connected to MQTT broker")
            return

        topic = f"{self.base_topic}{led_id}/brightness"
        payload = {"brightness": brightness, "timestamp": datetime.utcnow().isoformat()}

        try:
            self.client.publish(topic, json.dumps(payload))
            logger.info(f"Published brightness {brightness} for LED {led_id}")
        except Exception as e:
            logger.error(f"Error publishing LED brightness: {e}")

    @property
    def is_connected(self):
        """Check if client is currently connected"""
        return self.connected
