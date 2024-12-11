from telegram import Update
from telegram.ext import ContextTypes


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /start command"""
    await update.message.reply_text(
        "Ciao! I'm your LED-BOT controller.\n"
        "Use /login username password to access.\n"
        "Use /help per vedere to show all the possible commands."
    )


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /help command"""
    help_text = """
Available commands:

Basic Commands:
/start - Start the bot
/help - Show this help message
/login username password - Login to the system
/logout - Logout from the system

LED Control (for logged users only):
/ON led_id - Turn ON specific LED
/OFF led_id - Turn OFF specific LED

Example:
/login mario password123
/ON abc123
/OFF abc123
    """
    await update.message.reply_text(help_text)


async def echo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Echo handler that replies with the same message received"""
    await update.message.reply_text(
        "Non capisco questo comando. Usa /help per vedere i comandi disponibili."
    )
