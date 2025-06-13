import os
import threading
# request is needed to handle form data, jsonify to send back responses
from flask import Flask, render_template, jsonify, request
from tinydb import TinyDB # Import TinyDB
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# --- 1. DATABASE AND FLASK SETUP ---
app = Flask(__name__)
db = TinyDB('db.json') # This creates our database file
tournaments_table = db.table('tournaments') # This creates a 'table' for tournaments

# --- 2. FLASK WEB APP ROUTES ---

# This is the main page for the Mini App
@app.route('/')
def index():
    return render_template('index.html')

# This API endpoint now gets tournaments FROM THE DATABASE
@app.route('/api/tournaments')
def get_tournaments():
    all_tournaments = tournaments_table.all()
    # TinyDB assigns an ID automatically, we'll pass it to the frontend
    for tournament in all_tournaments:
        tournament['id'] = tournament.doc_id 
    return jsonify(all_tournaments)

# === NEW: API ENDPOINT TO CREATE A TOURNAMENT ===
@app.route('/api/tournaments/create', methods=['POST'])
def create_tournament():
    # Get the name from the form submitted by the frontend
    tournament_name = request.form['name']
    if tournament_name:
        # Insert the new tournament into our database table
        tournaments_table.insert({'name': tournament_name, 'status': 'New'})
        return jsonify({'status': 'success', 'message': 'Tournament created!'})
    return jsonify({'status': 'error', 'message': 'Name is required.'}), 400
# ============================================

# --- 3. TELEGRAM BOT LOGIC (No changes here) ---
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

# --- 4. START THE BOT IN A BACKGROUND THREAD (No changes here) ---
print("Starting bot thread...")
bot_thread = threading.Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()
print("Bot thread has been started.")
