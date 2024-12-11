from flask import Blueprint, request, jsonify, current_app
from telegram import Update
from src.application.telegram.config.settings import TELEGRAM_BLUE_PRINTS

webhook = Blueprint("webhook", __name__, url_prefix=TELEGRAM_BLUE_PRINTS)
application = None


def register_webhook(app):
    """Register LED API blueprint with Flask app"""
    app.register_blueprint(webhook)


def init_routes(app):
    """Initialize the routes with the Telegram application instance"""
    global application
    application = app


@webhook.route("/telegram", methods=["POST"])
def telegram_webhook():
    """Webhook endpoint for receiving updates from Telegram"""
    if request.method == "POST":
        update = Update.de_json(request.get_json(), application.bot)
        application.loop.run_until_complete(application.process_update(update))
    return "OK"


@webhook.route("/")
def index():
    """Root endpoint to check if the bot is active"""
    return "Bot is up and running!"
