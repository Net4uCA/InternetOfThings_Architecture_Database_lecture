from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from datetime import datetime
import re
from flask import current_app

# Dizionario per tenere traccia degli utenti loggati
# telegram_id -> user_id
logged_users = {}


async def login_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler foro /login username password"""
    try:
        telegram_id = update.effective_user.id
        if telegram_id in logged_users:
            await update.message.reply_text(
                "You are already logged in! Use /logout to exit."
            )
            return

        # Estrai username e password dal comando
        message = update.message.text.strip()
        match = re.match(r"/login\s+(\S+)\s+(\S+)", message)

        if not match:
            await update.message.reply_text(
                "Invalid format! Use: /login username password"
            )
            return

        username, password = match.groups()

        # Cerca l'utente nel database
        db_service = current_app.config["DB_SERVICE"]
        users = db_service.query_drs(
            "user", {"profile.username": username, "profile.password": password}
        )

        if not users:
            await update.message.reply_text("Invalid credentials!")
            return

        user = users[0]

        current_data = user["data"].copy()  # Copia i dati attuali
        current_data["last_login"] = datetime.utcnow()  # Aggiorna solo last_login

        db_service.update_dr(
            "user",
            user["_id"],
            {
                "data": current_data,  # Usa i dati aggiornati
                "metadata": {"updated_at": datetime.utcnow()},
            },
        )

        # Salva l'utente come loggato
        logged_users[telegram_id] = user["_id"]

        # Mostra messaggio di benvenuto con LED posseduti
        owned_leds = current_data.get("owned_leds", [])
        if owned_leds:
            led_list = "\n".join([f"- {led_id}" for led_id in owned_leds])
            await update.message.reply_text(f"Login successful! Your LEDs:\n{led_list}")
        else:
            await update.message.reply_text(
                "Login successful! You don't own any LEDs yet."
            )

    except Exception as e:
        await update.message.reply_text(f"Error during login: {str(e)}")


async def logout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /logout"""
    try:
        telegram_id = update.effective_user.id
        if telegram_id not in logged_users:
            await update.message.reply_text("Your are not logged!")
            return

        # Rimuovi l'utente dai loggati
        del logged_users[telegram_id]
        await update.message.reply_text("Logout executed!")

    except Exception as e:
        await update.message.reply_text(f"Error during the logout procedure: {str(e)}")


def check_auth(telegram_id: int) -> bool:
    """Verifica se un utente Telegram è autenticato"""
    return telegram_id in logged_users


def get_user_id(telegram_id: int) -> str:
    """Ottieni lo user_id dell'utente loggato"""
    return logged_users.get(telegram_id)
