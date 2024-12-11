from flask import Flask
from flask_cors import CORS
from src.virtualization.digital_replica.schema_registry import SchemaRegistry
from src.services.database_service import DatabaseService
from src.digital_twin.dt_factory import DTFactory
from src.application.api import register_api_blueprints
from config.config_loader import ConfigLoader
from src.application.led_apis import register_led_blueprint
from src.application.user_led_apis import register_user_blueprint
from src.application.mqtt_handler import LEDMQTTHandler

#### NEW IMPORTS #####
from pyngrok import ngrok
from telegram.ext import Application, CommandHandler, MessageHandler, filters
import asyncio
import nest_asyncio
from src.application.telegram.config.settings import (
    TELEGRAM_TOKEN,
    NGROK_TOKEN,
    WEBHOOK_PATH,
    TELEGRAM_BLUE_PRINTS,
)
from src.application.telegram.handlers.base_handlers import (
    start_handler,
    help_handler,
    echo_handler,
)
from src.application.telegram.routes.webhook_routes import register_webhook, init_routes
from src.application.telegram.handlers.login_handlers import (
    login_handler,
    logout_handler,
)
from src.application.telegram.handlers.led_handlers import (
    led_off_handler,
    led_on_handler,
)

######################

nest_asyncio.apply()
SERVER_PORT = 88


def setup_handlers(application):
    """Setup all the bot command handlers"""
    # Registra i base handlers
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, echo_handler)
    )
    application.add_handler(CommandHandler("login", login_handler))
    application.add_handler(CommandHandler("logout", logout_handler))
    application.add_handler(CommandHandler("OFF", led_off_handler))
    application.add_handler(CommandHandler("ON", led_on_handler))


class FlaskServer:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        self._init_components()
        self._register_blueprints()
        self.app.config["DEBUG"] = True
        self.ngrok_tunnel = None
        self.app.config["USE_RELOADER"] = False
        # Initialize MQTT config
        self.app.config["MQTT_CONFIG"] = {
            "broker": "broker.mqttdashboard.com",
            "port": 1883,
        }
        # Initialize MQTT handler
        self.app.mqtt_handler = LEDMQTTHandler(self.app)

    def _init_components(self):
        try:
            # Kill any existing ngrok process at startup
            import psutil

            for proc in psutil.process_iter(["pid", "name"]):
                if "ngrok" in proc.info["name"].lower():
                    try:
                        psutil.Process(proc.info["pid"]).terminate()
                        print(
                            f"Terminated existing ngrok process: PID {proc.info['pid']}"
                        )
                    except:
                        pass

            schema_registry = SchemaRegistry()
            schema_registry.load_schema("led", "src/virtualization/templates/led.yaml")
            schema_registry.load_schema(
                "user", "src/virtualization/templates/user.yaml"
            )

            # Load database configuration
            db_config = ConfigLoader.load_database_config()
            connection_string = ConfigLoader.build_connection_string(db_config)

            # Create a persistent event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            #### NGROK ########################################
            # Aspetta un momento per assicurarsi che i vecchi processi siano terminati
            import time

            time.sleep(2)

            # Setup ngrok
            ngrok.set_auth_token(NGROK_TOKEN)
            self.ngrok_tunnel = ngrok.connect(SERVER_PORT)
            webhook_url = (
                f"{self.ngrok_tunnel.public_url}{TELEGRAM_BLUE_PRINTS}{WEBHOOK_PATH}"
            )
            print(f"Webhook URL: {webhook_url}")
            ####################################################

            # TELEGRAM INITIALIZATION########################
            application = Application.builder().token(TELEGRAM_TOKEN).build()
            application.loop = loop
            setup_handlers(application)
            init_routes(application)
            loop.run_until_complete(application.initialize())
            loop.run_until_complete(application.start())
            loop.run_until_complete(application.bot.set_webhook(webhook_url))
            ################################################

            # Initialize DatabaseService
            db_service = DatabaseService(
                connection_string=connection_string,
                db_name=db_config["settings"]["name"],
                schema_registry=schema_registry,
            )
            db_service.connect()

            # Initialize DTFactory
            dt_factory = DTFactory(db_service, schema_registry)

            # Store references
            self.app.config["SCHEMA_REGISTRY"] = schema_registry
            self.app.config["DB_SERVICE"] = db_service
            self.app.config["DT_FACTORY"] = dt_factory

        except Exception as e:
            print(f"Initialization error: {str(e)}")
            if self.ngrok_tunnel:
                ngrok.disconnect(self.ngrok_tunnel.public_url)
            raise e

    def _register_blueprints(self):
        """Register all API blueprints"""
        register_api_blueprints(self.app)
        register_led_blueprint(self.app)
        register_user_blueprint(self.app)
        register_webhook(self.app)  # ----> TELEGRAM

    def run(self, host="0.0.0.0", port=SERVER_PORT):
        """Run the Flask server"""
        try:
            self.app.mqtt_handler.start()
            self.app.run(host=host, port=port, use_reloader=False)
        finally:
            if "DB_SERVICE" in self.app.config:
                self.app.config["DB_SERVICE"].disconnect()
            if self.ngrok_tunnel:
                ngrok.disconnect(self.ngrok_tunnel.public_url)


if __name__ == "__main__":
    server = FlaskServer()
    server.run()
