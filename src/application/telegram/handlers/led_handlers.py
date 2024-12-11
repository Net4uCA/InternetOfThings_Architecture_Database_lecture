from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from datetime import datetime
import re
from flask import current_app
from src.application.telegram.handlers.login_handlers import (
    check_auth,
    logged_users,
)


async def led_on_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /ON command"""
    try:
        # Check if user is authenticated
        telegram_id = update.effective_user.id
        if not check_auth(telegram_id):
            await update.message.reply_text(
                "Please login first using /login username password"
            )
            return

        # Extract LED ID from command
        message = update.message.text.strip()
        command_pattern = r"/ON\s+([^\s]+)"  # Pattern for ON command
        match = re.match(command_pattern, message)

        if not match:
            await update.message.reply_text(
                "Invalid format! Use: /ON <led_id>\n"
                "Example: /ON 47a0c269-5f20-4209-97ea-d360f347941c"
            )
            return

        led_id = match.group(1)

        # Verify LED ownership
        db_service = current_app.config["DB_SERVICE"]
        user = db_service.get_dr("user", logged_users[telegram_id])

        if led_id not in user["data"]["owned_leds"]:
            await update.message.reply_text("You don't own this LED!")
            return

        # Get LED status
        led = db_service.get_dr("led", led_id)
        if not led:
            await update.message.reply_text("LED not found!")
            return

        # Update LED state
        current_time = datetime.utcnow()
        update_data = {
            "data": {
                "state": "on",
                "controlled_by": f"telegram_{telegram_id}",
                "measurements": led["data"]["measurements"]
                + [{"type": "state_change", "value": 1.0, "timestamp": current_time}],
            },
            "metadata": {"updated_at": current_time, "last_state_change": current_time},
        }

        db_service.update_dr("led", led_id, update_data)
        if current_app.mqtt_handler.is_connected:
            current_app.mqtt_handler.publish_led_state(led_id, "on")
            await update.message.reply_text(f"LED {led_id} turned ON!")

    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        print(f"LED ON error: {str(e)}")


async def led_off_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /OFF command"""
    try:
        # Check if user is authenticated
        telegram_id = update.effective_user.id
        if not check_auth(telegram_id):
            await update.message.reply_text(
                "Please login first using /login username password"
            )
            return

        # Extract LED ID from command
        message = update.message.text.strip()
        command_pattern = r"/OFF\s+([^\s]+)"  # Pattern for OFF command
        match = re.match(command_pattern, message)

        if not match:
            await update.message.reply_text(
                "Invalid format! Use: /OFF <led_id>\n"
                "Example: /OFF 47a0c269-5f20-4209-97ea-d360f347941c"
            )
            return

        led_id = match.group(1)

        # Verify LED ownership
        db_service = current_app.config["DB_SERVICE"]
        user = db_service.get_dr("user", logged_users[telegram_id])

        if led_id not in user["data"]["owned_leds"]:
            await update.message.reply_text("You don't own this LED!")
            return

        # Get LED status
        led = db_service.get_dr("led", led_id)
        if not led:
            await update.message.reply_text("LED not found!")
            return

        # Update LED state
        current_time = datetime.utcnow()
        update_data = {
            "data": {
                "state": "off",
                "controlled_by": f"telegram_{telegram_id}",
                "measurements": led["data"]["measurements"]
                + [{"type": "state_change", "value": 0.0, "timestamp": current_time}],
            },
            "metadata": {"updated_at": current_time, "last_state_change": current_time},
        }

        db_service.update_dr("led", led_id, update_data)
        if (
            hasattr(current_app, "mqtt_handler")
            and current_app.mqtt_handler.is_connected
        ):
            current_app.mqtt_handler.publish_led_state(led_id, "off")
            await update.message.reply_text(f"LED {led_id} turned OFF!")

    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)}")
        print(f"LED OFF error: {str(e)}")
