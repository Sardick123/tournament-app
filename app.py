import os
import threading
from flask import Flask, render_template, jsonify, request, abort
from tinydb import TinyDB, Query
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

# --- 1. DATABASE AND FLASK SETUP ---
app = Flask(__name__)
db = TinyDB('db.json')
tournaments_table = db.table('tournaments')

# --- 2. FLASK WEB APP ROUTES ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/tournament/<int:tournament_id>')
def tournament_detail(tournament_id):
    tournament = tournaments_table.get(doc_id=tournament_id)
    if tournament:
        tournament['id'] = tournament.doc_id
        return render_template('tournament_detail.html', tournament=tournament)
    else:
        return abort(404)

# --- 3. API ENDPOINTS ---
@app.route('/api/tournaments')
def get_tournaments():
    all_tournaments = tournaments_table.all()
    for t in all_tournaments:
        t['id'] = t.doc_id
    return jsonify(all_tournaments)

@app.route('/api/tournaments/<int:tournament_id>/teams')
def get_teams(tournament_id):
    tournament = tournaments_table.get(doc_id=tournament_id)
    if tournament:
        return jsonify(tournament.get('teams', []))
    return jsonify({'status': 'error', 'message': 'Tournament not found'}), 404

@app.route('/api/tournaments/create', methods=['POST'])
def create_tournament():
    tournament_name = request.form['name']
    if tournament_name:
        new_tournament = {'name': tournament_name, 'status': 'New', 'teams': []}
        tournaments_table.insert(new_tournament)
        return jsonify({'status': 'success', 'message': 'Tournament created!'})
    return jsonify({'status': 'error', 'message': 'Name is required.'}), 400

@app.route('/api/tournaments/<int:tournament_id>/add_team', methods=['POST'])
def add_team(tournament_id):
    tournament = tournaments_table.get(doc_id=tournament_id)
    team_name = request.form['name']
    if tournament and team_name:
        current_teams = tournament.get('teams', [])
        current_teams.append({'name': team_name})
        tournaments_table.update({'teams': current_teams}, doc_ids=[tournament_id])
        return jsonify({'status': 'success', 'message': 'Team added!'})
    return jsonify({'status': 'error', 'message': 'Invalid request'}), 400

# --- 4. TELEGRAM BOT LOGIC ---
def get_webapp_url():
    return os.environ.get("WEBAPP_URL")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("🏆 Open Tournament App 🏆", web_app={'url': get_webapp_url()})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Welcome!", reply_markup=reply_markup)

def run_bot():
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable not set!")
        return
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    print("Bot is starting to poll...")
    application.run_polling()
    print("Bot has stopped.")

# --- 5. START THE BOT IN A BACKGROUND THREAD ---
print("Starting bot thread...")
bot_thread = threading.Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()
print("Bot thread has been started.")
