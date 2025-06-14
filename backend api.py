# backend/api.py
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from db_utils import (
    init_db,
    get_tournaments,
    get_tournament_details,
    add_registration
)

app = Flask(__name__)
# Allow requests from any origin, which is fine for a Telegram Mini App
CORS(app)

@app.route('/api/tournaments', methods=['GET'])
def api_get_tournaments():
    """API endpoint to get a list of all tournaments."""
    tournaments_data = get_tournaments()
    return jsonify(tournaments_data)

@app.route('/api/tournaments/<string:tournament_id>', methods=['GET'])
def api_get_tournament_details(tournament_id):
    """API endpoint to get details for a specific tournament."""
    details = get_tournament_details(tournament_id)
    if details:
        return jsonify(details)
    else:
        return jsonify({"error": "Tournament not found"}), 404

@app.route('/api/tournaments/<string:tournament_id>/join', methods=['POST'])
def api_join_tournament(tournament_id):
    """API endpoint for a user to join a tournament."""
    data = request.json
    user_id = data.get('user_id')
    username = data.get('username')

    if not user_id or not username:
        return jsonify({"error": "User ID and username are required"}), 400

    result = add_registration(tournament_id, user_id, username)
    
    if result["success"]:
        return jsonify(result)
    else:
        return jsonify(result), 409 # 409 Conflict is a good code for "already exists"

# A special one-time command to initialize the database
@app.cli.command("init-db")
def init_db_command():
    init_db()

if __name__ == '__main__':
    # This part is for local development only.
    # Gunicorn will be used in production on Render.
    os.environ['DATABASE_URL'] = 'postgresql://user:password@host:port/dbname' # Replace with your local or Render DB URL
    app.run(host='0.0.0.0', port=5001, debug=True)

