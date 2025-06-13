import os
import threading
from flask import Flask, render_template, jsonify  # <-- Add jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# --- 1. FLASK WEB APP SETUP ---
app = Flask(__name__)

# This is the main page for the Mini App
@app.route('/')
def index():
    return render_template('index.html')

# === NEW: API ENDPOINT TO GET TOURNAMENTS ===
@app.route('/api/tournaments')
def get_tournaments():
    # For now, we'll use a hardcoded list of tournaments.
    # Later, this data can come from a database.
    tournaments = [
        {"id": 1, "name": "Friday Night Efootball", "slots": "8/16", "status": "Open"},
        {"id": 2, "name": "Weekend Champions League", "slots": "15/16", "status": "Open"},
        {"id": 3, "name": "Monthly Pro Cup", "slots": "32/32", "status": "Full"}
    ]
    return jsonify(tournaments)
# ============================================

# --- 2. TELEGRAM BOT LOGIC ---
def get_webapp_url():
    return os.environ.get("WEBAPP_URL", "https://your-app-name.onrender.com")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("🏆 Open Tournament App 🏆", web_app={'url': get_webapp_url()})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Welcome! Click the button below to open the tournament app.",
        reply_markup=reply_markup
    )

def run_bot():
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable not set!")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    print("Bot is starting to poll for updates...")
    application.run_polling()
    print("Bot has stopped.")

# --- 3. START THE BOT IN A BACKGROUND THREAD ---
print("Starting bot thread...")
bot_thread = threading.Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()
print("Bot thread has been started.")
