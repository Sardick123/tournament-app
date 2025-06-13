import os
import threading
import itertools
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

@app.route('/api/tournaments/<int:tournament_id>/fixtures')
def get_fixtures(tournament_id):
    tournament = tournaments_table.get(doc_id=tournament_id)
    if tournament:
        return jsonify(tournament.get('fixtures', []))
    return jsonify({'status': 'error', 'message': 'Tournament not found'}), 404

@app.route('/api/tournaments/create', methods=['POST'])
def create_tournament():
    tournament_name = request.form['name']
    if tournament_name:
        new_tournament = {'name': tournament_name, 'status': 'New', 'teams': [], 'fixtures': []}
        tournaments_table.insert(new_tournament)
        return jsonify({'status': 'success', 'message': 'Tournament created!'})
    return jsonify({'status': 'error', 'message': 'Name is required.'}), 400

@app.route('/api/tournaments/<int:tournament_id>/add_team', methods=['POST'])
def add_team(tournament_id):
    # ... (this function is unchanged)
    tournament = tournaments_table.get(doc_id=tournament_id)
    team_name = request.form['name']
    if tournament and team_name:
        current_teams = tournament.get('teams', [])
        current_teams.append({'name': team_name})
        tournaments_table.update({'teams': current_teams}, doc_ids=[tournament_id])
        return jsonify({'status': 'success', 'message': 'Team added!'})
    return jsonify({'status': 'error', 'message': 'Invalid request'}), 400

@app.route('/api/tournaments/<int:tournament_id>/generate_fixtures', methods=['POST'])
def generate_fixtures(tournament_id):
    # ... (this function is unchanged)
    tournament = tournaments_table.get(doc_id=tournament_id)
    if not tournament or len(tournament.get('teams', [])) < 2:
        return jsonify({'status': 'error', 'message': 'Add at least 2 teams to generate fixtures.'}), 400
    if len(tournament.get('fixtures', [])) > 0:
        return jsonify({'status': 'error', 'message': 'Fixtures have already been generated.'}), 400
    team_names = [team['name'] for team in tournament.get('teams', [])]
    all_matchups = list(itertools.combinations(team_names, 2))
    fixtures = []
    for match in all_matchups:
        fixtures.append({'home_team': match[0], 'away_team': match[1], 'home_score': None, 'away_score': None})
    tournaments_table.update({'fixtures': fixtures, 'status': 'In Progress'}, doc_ids=[tournament_id])
    return jsonify({'status': 'success', 'message': f'{len(fixtures)} fixtures generated successfully!'})

# === NEW: API ENDPOINT TO UPDATE A FIXTURE'S SCORE ===
@app.route('/api/tournaments/<int:tournament_id>/update_fixture/<int:fixture_index>', methods=['POST'])
def update_fixture(tournament_id, fixture_index):
    tournament = tournaments_table.get(doc_id=tournament_id)

    home_score_str = request.form.get('home_score')
    away_score_str = request.form.get('away_score')

    # Ensure that if the input is an empty string, we store it as None (not played)
    home_score = int(home_score_str) if home_score_str else None
    away_score = int(away_score_str) if away_score_str else None

    if tournament and fixture_index < len(tournament['fixtures']):
        # Update the specific fixture in the list
        tournament['fixtures'][fixture_index]['home_score'] = home_score
        tournament['fixtures'][fixture_index]['away_score'] = away_score

        # Save the entire updated tournament document back to the database
        tournaments_table.update(tournament, doc_ids=[tournament_id])
        return jsonify({'status': 'success', 'message': 'Score updated!'})

    return jsonify({'status': 'error', 'message': 'Invalid request'}), 400
# =======================================================

# --- 4. & 5. TELEGRAM BOT & THREADING (No changes here) ---
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

print("Starting bot thread...")
bot_thread = threading.Thread(target=run_bot)
bot_thread.daemon = True
bot_thread.start()
print("Bot thread has been started.")
