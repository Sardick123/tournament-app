import os
import logging
import uuid
import sqlite3
import re  # For the escape function
import random  # For shuffling players
import shlex
from datetime import datetime, timezone
import math
from flask import Flask
from threading import Thread
from telegram.helpers import escape_markdown
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
# Define the persistent data directory Render will provide at /var/data
DATA_DIR = "/data"
DB_NAME = os.path.join(DATA_DIR, "tournaments.db")

# Ensure the data directory exists when the bot starts
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- Points System for Round Robin / Group Stage / Swiss ---
POINTS_FOR_WIN = 3
POINTS_FOR_DRAW = 1
POINTS_FOR_LOSS = 0


def escape_markdown_v2(text: str) -> str:
    """Escapes characters that have special meaning in MarkdownV2."""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r"_*\[\]()~`>#\+\-=|{}\.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)


def dict_factory(cursor, row):
    """Converts SQL rows to dictionaries."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def init_db():
    """Initializes or verifies the database schema."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tournaments (
            id TEXT PRIMARY KEY,
            creator_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            game TEXT,
            participants INTEGER,
            type TEXT, -- "Single Elimination", "Round Robin", "Group Stage & Knockout", "Swiss"
            status TEXT, -- 'pending', 'ongoing', 'completed', 'cancelled', 'ongoing_knockout' (for Swiss)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            winner_user_id INTEGER,
            winner_username TEXT,
            tournament_time TEXT,
            penalties TEXT,
            extra_time TEXT,
            conditions TEXT,
            group_chat_id INTEGER DEFAULT NULL,
            num_groups INTEGER DEFAULT NULL,
            num_swiss_rounds INTEGER DEFAULT NULL,
            current_swiss_round INTEGER DEFAULT 0,
            swiss_knockout_qualifiers INTEGER DEFAULT NULL -- NEW: For Swiss tournaments to define knockout size
        )
    """
    )
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_achievements (
            achievement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            achievement_code TEXT NOT NULL,
            description TEXT, -- NEW: To store custom badge text
            unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tournament_id TEXT
        )
    ''')
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS registrations (
            registration_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            registration_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
            UNIQUE (tournament_id, user_id)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id TEXT NOT NULL,
            round_number INTEGER NOT NULL,
            match_in_round_index INTEGER NOT NULL,
            player1_user_id INTEGER,
            player1_username TEXT,
            player2_user_id INTEGER,
            player2_username TEXT,
            winner_user_id INTEGER,
            score TEXT,
            status TEXT NOT NULL,
            next_match_id INTEGER,
            group_id INTEGER DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
            FOREIGN KEY (next_match_id) REFERENCES matches (match_id),
            FOREIGN KEY (group_id) REFERENCES groups_tournament (group_id)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS leaderboard_points (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            points INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            matches_played INTEGER DEFAULT 0,
            match_wins INTEGER DEFAULT 0,
            last_win_timestamp TIMESTAMP
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS round_robin_standings (
            tournament_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            games_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            goals_for INTEGER DEFAULT 0,
            goals_against INTEGER DEFAULT 0,
            goal_difference INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            PRIMARY KEY (tournament_id, user_id),
            FOREIGN KEY (tournament_id) REFERENCES tournaments (id)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS groups_tournament (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id TEXT NOT NULL,
            group_name TEXT NOT NULL,
            FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
            UNIQUE (tournament_id, group_name)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS group_participants (
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            FOREIGN KEY (group_id) REFERENCES groups_tournament (group_id),
            UNIQUE (group_id, user_id)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS group_stage_standings (
            tournament_id TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            games_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            goals_for INTEGER DEFAULT 0,
            goals_against INTEGER DEFAULT 0,
            goal_difference INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            PRIMARY KEY (tournament_id, group_id, user_id),
            FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
            FOREIGN KEY (group_id) REFERENCES groups_tournament (group_id)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS score_submissions (
            submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            score_p1 INTEGER NOT NULL, -- Score for player1 in the match
            score_p2 INTEGER NOT NULL, -- Score for player2 in the match
            submission_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches (match_id),
            UNIQUE (match_id, user_id) -- A player can only submit score once per match
        )
    """
    )
    conn.commit()
    conn.close()
    logger.info(
        "Database initialized/verified (including group stage, score_submissions, and Swiss fields)."
    )


# --- Database Helper Functions ---
def add_tournament_to_db(details: dict) -> bool:
    """Adds a new tournament to the database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO tournaments (
                id, creator_id, name, game, participants, type, status,
                tournament_time, penalties, extra_time, conditions, group_chat_id,
                num_groups, num_swiss_rounds, current_swiss_round, swiss_knockout_qualifiers
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                details["id"],
                details["creator_id"],
                details["name"],
                details.get("game"),
                details.get("participants"),
                details.get("type"),
                details.get("status"),
                details.get("tournament_time"),
                details.get("penalties"),
                details.get("extra_time"),
                details.get("conditions"),
                details.get("group_chat_id"),
                details.get("num_groups"),
                details.get("num_swiss_rounds"),
                details.get("current_swiss_round", 0),
                details.get("swiss_knockout_qualifiers"),
            ),
        )
        conn.commit()
        logger.info(f"T_ID {details['id']} added to DB with extra details.")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB add_tournament: {e}")
        return False
    finally:
        conn.close()


def update_tournament_swiss_round(
        tournament_id: str, new_round_num: int) -> bool:
    """Updates the current_swiss_round for a Swiss tournament."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE tournaments SET current_swiss_round = ? WHERE id = ?",
            (new_round_num, tournament_id),
        )
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(
            f"DB update_tournament_swiss_round for {tournament_id}: {e}")
        return False
    finally:
        conn.close()


def get_match_history_from_db(
        user_id: int, page: int = 1, limit: int = 5) -> tuple[list, int]:
    """Fetches a paginated match history for a player. Returns (matches, total_count)."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    offset = (page - 1) * limit

    try:
        # First, get the total count of matches for the player
        cursor.execute(
            "SELECT COUNT(*) FROM matches WHERE (player1_user_id = ? OR player2_user_id = ?) AND status = 'completed'",
            (user_id, user_id)
        )
        total_count = cursor.fetchone()['COUNT(*)']

        # Now, get the paginated results with tournament names
        cursor.execute("""
            SELECT
                m.player1_user_id, m.player1_username, m.player2_user_id,
                m.player2_username, m.winner_user_id, m.score, m.created_at, t.name as tournament_name
            FROM matches as m
            JOIN tournaments as t ON m.tournament_id = t.id
            WHERE (m.player1_user_id = ? OR m.player2_user_id = ?) AND m.status = 'completed'
            ORDER BY m.match_id DESC
            LIMIT ? OFFSET ?;
        """, (user_id, user_id, limit, offset))

        matches = cursor.fetchall()
        return matches, total_count

    except sqlite3.Error as e:
        logger.error(f"DB error fetching match history for {user_id}: {e}")
        return [], 0
    finally:
        conn.close()


def get_tournaments_from_db(
    limit: int = 10, creator_id: int | None = None, group_chat_id: int | None = None
) -> list:
    """
    Fetches a list of recent tournaments from the database.
    Can be filtered by creator_id and/or group_chat_id.
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    tournaments = []
    try:
        query = "SELECT * FROM tournaments"
        params = []
        where_clauses = []

        if creator_id is not None:
            where_clauses.append("creator_id = ?")
            params.append(creator_id)

        if group_chat_id is not None:
            where_clauses.append("group_chat_id = ?")
            params.append(group_chat_id)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, tuple(params))
        tournaments = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_tournaments: {e}")
    finally:
        conn.close()
    return tournaments


def get_tournament_details_by_id(tournament_id: str) -> dict | None:
    """Fetches details for a specific tournament by its ID."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM tournaments WHERE id = ?", (tournament_id,))
        return cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"DB get_tournament_details for {tournament_id}: {e}")
        return None
    finally:
        conn.close()


def update_global_stats_for_players(
        player_id: int, username: str, is_winner: bool):
    """Updates a player's global match stats after a game."""
    if not player_id:
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        # Ensure the player exists in the leaderboard, otherwise create an
        # entry
        cursor.execute(
            "INSERT OR IGNORE INTO leaderboard_points (user_id, username) VALUES (?, ?)",
            (player_id, username if username else f"User_{player_id}"),
        )

        # Now, update their stats
        win_increment = 1 if is_winner else 0
        cursor.execute(
            """
            UPDATE leaderboard_points
            SET matches_played = matches_played + 1,
                match_wins = match_wins + ?
            WHERE user_id = ?
            """,
            (win_increment, player_id),
        )
        conn.commit()
        logger.info(f"Updated global stats for player {player_id}.")
    except sqlite3.Error as e:
        logger.error(
            f"DB error updating global stats for player {player_id}: {e}")
    finally:
        conn.close()


def add_registration_to_db(
    tournament_id: str, user_id: int, username: str | None
) -> bool:
    """Registers a user for a tournament."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        display_name = username if username else f"User_{user_id}"
        cursor.execute(
            "INSERT INTO registrations (tournament_id, user_id, username) VALUES (?, ?, ?)",
            (tournament_id, user_id, display_name),
        )
        conn.commit()
        logger.info(
            f"User {user_id} ({display_name}) registered for T_ID {tournament_id}."
        )
        return True
    except sqlite3.IntegrityError:
        logger.info(f"User {user_id} already registered for {tournament_id}.")
        return False
    except sqlite3.Error as e:
        logger.error(f"DB add_registration: {e}")
        return False
    finally:
        conn.close()


def is_user_registered(tournament_id: str, user_id: int) -> bool:
    """Checks if a user is registered for a specific tournament."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM registrations WHERE tournament_id = ? AND user_id = ?",
            (tournament_id, user_id),
        )
        return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"DB is_user_registered: {e}")
        return False
    finally:
        conn.close()


def get_registration_count(tournament_id: str) -> int:
    """Gets the number of registered players for a tournament."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM registrations WHERE tournament_id = ?",
            (tournament_id,),
        )
        return cursor.fetchone()[0]
    except sqlite3.Error as e:
        logger.error(f"DB get_registration_count: {e}")
        return 0
    finally:
        conn.close()


def get_registered_players(tournament_id: str) -> list:
    """Gets the list of registered players for a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    players = []
    try:
        cursor.execute(
            "SELECT user_id, username FROM registrations WHERE tournament_id = ?",
            (tournament_id,),
        )
        players = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_registered_players for {tournament_id}: {e}")
    finally:
        conn.close()
    return players


def get_player_username_by_id(user_id: int) -> str | None:
    """Retrieves a player's username by their user ID."""
    if not user_id:
        return "N/A"
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT username FROM registrations WHERE user_id = ? AND username IS NOT NULL ORDER BY registration_time DESC LIMIT 1",
            (user_id,),
        )
        result = cursor.fetchone()
        return (
            result["username"] if result and result["username"] else f"User_{user_id}"
        )
    except sqlite3.Error as e:
        logger.error(f"DB get_player_username_by_id for {user_id}: {e}")
        return f"User_{user_id}"
    finally:
        conn.close()


def update_tournament_status(
    tournament_id: str,
    new_status: str,
    winner_user_id: int | None = None,
    winner_username: str | None = None,
) -> bool:
    """Updates the status of a tournament, optionally setting a winner."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        if new_status == "completed" and winner_user_id:
            w_display_name = (
                winner_username
                if winner_username
                else get_player_username_by_id(winner_user_id)
            )
            cursor.execute(
                "UPDATE tournaments SET status = ?, winner_user_id = ?, winner_username = ? WHERE id = ?",
                (new_status, winner_user_id, w_display_name, tournament_id),
            )
        else:
            cursor.execute(
                "UPDATE tournaments SET status = ? WHERE id = ?",
                (new_status, tournament_id),
            )
        conn.commit()
        logger.info(
            f"T_ID {tournament_id} status updated to {new_status}. Winner: {
                winner_username if winner_username else 'N/A'}"
        )
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"DB update_tournament_status for {tournament_id}: {e}")
        return False
    finally:
        conn.close()


def add_match_to_db(match_details: dict) -> int | None:
    """Adds a new match to the database, including the creation timestamp."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        # Get the current time to be inserted explicitly
        now_utc = datetime.now(timezone.utc)

        cursor.execute("""
            INSERT INTO matches (
                tournament_id, round_number, match_in_round_index,
                player1_user_id, player1_username,
                player2_user_id, player2_username,
                winner_user_id, score, status, next_match_id, group_id,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            match_details['tournament_id'], match_details['round_number'], match_details['match_in_round_index'],
            match_details.get('player1_user_id'), match_details.get(
                'player1_username'),
            match_details.get('player2_user_id'), match_details.get(
                'player2_username'),
            match_details.get('winner_user_id'), match_details.get('score'),
            match_details['status'], match_details.get(
                'next_match_id'), match_details.get('group_id'),
            now_utc  # Explicitly providing the timestamp
        ))
        conn.commit()
        match_id = cursor.lastrowid
        logger.info(
            f"Match {match_id} for T_ID {
                match_details['tournament_id']} added. Status: {
                match_details['status']}.")
        return match_id
    except sqlite3.Error as e:
        logger.error(f"DB add_match: {e} with details {match_details}")
        return None
    finally:
        conn.close()


def get_matches_for_tournament(
    tournament_id: str,
    match_status: str | None = None,
    round_number: int | None = None,
    group_id: int | None = None,
) -> list:
    """Fetches matches for a given tournament, with optional status, round, and group filters."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    matches_list = []
    query = "SELECT * FROM matches WHERE tournament_id = ?"
    params = [tournament_id]
    if match_status:
        query += " AND status = ?"
        params.append(match_status)
    if round_number is not None:
        query += " AND round_number = ?"
        params.append(round_number)
    if group_id is not None:  # For group stage matches
        query += " AND group_id = ?"
        params.append(group_id)
    elif group_id is None:  # For non-group matches (SE, Swiss, KO)
        query += " AND group_id IS NULL"
    query += " ORDER BY round_number, match_in_round_index"
    try:
        cursor.execute(query, tuple(params))
        matches_list = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_matches_for_tournament {tournament_id}: {e}")
    finally:
        conn.close()
    return matches_list


def get_match_details_by_match_id(match_id: int) -> dict | None:
    """Fetches details for a specific match by its ID."""
    if match_id is None:
        return None
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,))
        match = cursor.fetchone()
        return match
    except sqlite3.Error as e:
        logger.error(f"Database error while fetching match {match_id}: {e}")
        return None
    finally:
        conn.close()


# A dictionary defining all possible achievements
ACHIEVEMENTS = {
    'TOURNEY_CHAMPION': '🏆 Tournament Champion',
    'FIRST_WIN': '🥇 First Victory',
    'SURVIVOR': '🧗 Group Stage Survivor',
    'UNDEFEATED_CHAMP': '💪 Undefeated Champion',
    'VETERAN_5': '🛡️ 5-Tournament Veteran'
}


def award_achievement(user_id: int, achievement_code: str, tournament_id: str | None = None, description: str | None = None):
    """Awards a player an achievement. Now supports custom descriptions."""
    if achievement_code not in ACHIEVEMENTS and achievement_code != 'CUSTOM':
        return

    # For automatic achievements, get the description from our dictionary
    if not description and achievement_code in ACHIEVEMENTS:
        description = ACHIEVEMENTS[achievement_code]

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        # We no longer use IGNORE because a player can have multiple CUSTOM badges
        cursor.execute(
            "INSERT INTO player_achievements (user_id, achievement_code, description, tournament_id) VALUES (?, ?, ?, ?)",
            (user_id, achievement_code, description, tournament_id)
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Awarded achievement '{description}' to user {user_id}")
    except sqlite3.Error as e:
        logger.error(f"DB award_achievement failed: {e}")
    finally:
        conn.close()

def get_player_achievements(user_id: int) -> list:
    """Gets a list of all achievements (dictionaries) earned by a player."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        # Fetch the description as well
        cursor.execute("SELECT description FROM player_achievements WHERE user_id = ?", (user_id,))
        return [row['description'] for row in cursor.fetchall()]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def get_h2h_stats_from_db(user1_id: int, user2_id: int) -> dict | None:
    """Fetches head-to-head match statistics between two players, now handling walkovers."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    
    stats = {
        'user1_wins': 0,
        'user2_wins': 0,
        'draws': 0,
        'recent_matches': []
    }
    
    try:
        cursor.execute("""
            SELECT winner_user_id, player1_user_id, score, tournament_id
            FROM matches
            WHERE
                status = 'completed' AND
                (
                    (player1_user_id = ? AND player2_user_id = ?) OR
                    (player1_user_id = ? AND player2_user_id = ?)
                )
            ORDER BY match_id DESC
        """, (user1_id, user2_id, user2_id, user1_id))
        
        all_matches = cursor.fetchall()
        
        if not all_matches:
            return None # Return None if they've never played

        # Process the stats
        for match in all_matches:
            if match['winner_user_id'] == user1_id:
                stats['user1_wins'] += 1
            elif match['winner_user_id'] == user2_id:
                stats['user2_wins'] += 1
            elif match['winner_user_id'] is None:
                stats['draws'] += 1

            # Get last 3 recent matches
            if len(stats['recent_matches']) < 3:
                original_score = match.get('score', 'N/A')

                # --- NEW: Safely determine score from user1's perspective ---
                final_score = original_score
                if match['player1_user_id'] != user1_id:
                    try:
                        # Try to reverse the score, e.g., "1-2" -> "2-1"
                        p2_score, p1_score = original_score.split('-')
                        final_score = f"{p1_score}-{p2_score}"
                    except (ValueError, AttributeError):
                        # This will catch "W/O" or other non-standard scores and leave them as is
                        final_score = original_score
                # --- END OF NEW LOGIC ---
                
                t_details = get_tournament_details_by_id(match['tournament_id'])
                t_name = t_details['name'] if t_details else 'a tournament'
                
                stats['recent_matches'].append({'tournament_name': t_name, 'score': final_score})

        return stats
        
    except sqlite3.Error as e:
        logger.error(f"DB error fetching H2H stats for {user1_id} vs {user2_id}: {e}")
        return {}
    finally:
        conn.close()


def get_player_stats_from_db(user_id: int) -> dict | None:
    """
    Fetches and computes all relevant stats for a given player ID.
    Returns a dictionary with stats or None if player not found.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    stats = {}

    try:
        # Check if player exists
        cursor.execute(
            "SELECT 1 FROM registrations WHERE user_id = ?", (user_id,))
        if cursor.fetchone() is None:
            return None  # Player has never registered for any tournament

        # 1. Tournaments Won
        cursor.execute(
            "SELECT COUNT(*) FROM tournaments WHERE winner_user_id = ?", (user_id,)
        )
        stats["tournaments_won"] = cursor.fetchone()[0]

        # 2. Tournaments Played
        cursor.execute(
            "SELECT COUNT(DISTINCT tournament_id) FROM registrations WHERE user_id = ?",
            (user_id,),
        )
        stats["tournaments_played"] = cursor.fetchone()[0]

        # 3. Matches Played
        cursor.execute(
            "SELECT COUNT(*) FROM matches WHERE (player1_user_id = ? OR player2_user_id = ?) AND status = 'completed'",
            (user_id, user_id),
        )
        stats["matches_played"] = cursor.fetchone()[0]

        # 4. Matches Won
        cursor.execute(
            "SELECT COUNT(*) FROM matches WHERE winner_user_id = ? AND status = 'completed'",
            (user_id,),
        )
        stats["matches_won"] = cursor.fetchone()[0]

        # 5. Compute derived stats
        stats["matches_lost"] = stats["matches_played"] - stats["matches_won"]
        if stats["matches_played"] > 0:
            stats["win_rate"] = (
                stats["matches_won"] / stats["matches_played"]) * 100
        else:
            stats["win_rate"] = 0
        stats['achievements'] = get_player_achievements(user_id)

        return stats

    except sqlite3.Error as e:
        logger.error(f"DB error fetching stats for player {user_id}: {e}")
        return {}  # Return empty dict on error
    finally:
        conn.close()


def get_final_match_details(tournament_id: str) -> dict | None:
    """Fetches the details of the final completed match for a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        # For Single Elim/Knockout, the final match is the one with no
        # next_match_id
        cursor.execute(
            """
            SELECT * FROM matches
            WHERE tournament_id = ? AND status = 'completed' AND next_match_id IS NULL
            ORDER BY round_number DESC, match_in_round_index DESC LIMIT 1
        """,
            (tournament_id,),
        )
        final_match = cursor.fetchone()
        if (
            not final_match
        ):  # Fallback if next_match_id logic wasn't perfect or for other types
            cursor.execute(
                """
                SELECT * FROM matches
                WHERE tournament_id = ? AND status = 'completed'
                ORDER BY round_number DESC, match_id DESC LIMIT 1
            """,
                (tournament_id,),
            )
            final_match = cursor.fetchone()
        return final_match
    except sqlite3.Error as e:
        logger.error(
            f"DB get_final_match_details for T_ID {tournament_id}: {e}")
        return None
    finally:
        conn.close()


def get_matches_won_by_player(tournament_id: str, player_id: int) -> list:
    """Fetches all matches won by a specific player in a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    matches_won = []
    try:
        cursor.execute(
            """
            SELECT * FROM matches
            WHERE tournament_id = ? AND winner_user_id = ? AND status = 'completed' AND player1_user_id IS NOT NULL AND player2_user_id IS NOT NULL
            ORDER BY round_number ASC
        """,
            (tournament_id, player_id),
        )
        matches_won = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(
            f"DB get_matches_won_by_player for T_ID {tournament_id}, P_ID {player_id}: {e}"
        )
    finally:
        conn.close()
    return matches_won


def update_leaderboard(
    winner_user_id: int, winner_username: str, points_to_add: int = 1
):
    """Updates the global leaderboard with points for a winner."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        current_display_name = winner_username
        if not current_display_name:
            current_display_name = get_player_username_by_id(winner_user_id)
        if not current_display_name:
            current_display_name = f"User_{winner_user_id}"

        now_utc = datetime.now(timezone.utc)

        original_row_factory = conn.row_factory
        conn.row_factory = (
            None  # Temporarily disable row_factory for fetchone to get a tuple
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT points, wins FROM leaderboard_points WHERE user_id = ?",
            (winner_user_id,),
        )
        data_tuple = cursor.fetchone()
        conn.row_factory = original_row_factory  # Restore row_factory

        if data_tuple:
            current_points = data_tuple[0] if data_tuple[0] is not None else 0
            current_wins = data_tuple[1] if data_tuple[1] is not None else 0
            new_points = current_points + points_to_add
            new_wins = current_wins + 1
            cursor.execute(
                """
                UPDATE leaderboard_points
                SET points = ?, wins = ?, username = ?, last_win_timestamp = ?
                WHERE user_id = ?
            """,
                (new_points, new_wins, current_display_name, now_utc, winner_user_id),
            )
            logger.info(
                f"Leaderboard updated for user {winner_user_id} ({current_display_name}): {new_points} points, {new_wins} wins."
            )
        else:
            cursor.execute(
                """
                INSERT INTO leaderboard_points (user_id, username, points, wins, last_win_timestamp)
                VALUES (?, ?, ?, ?, ?)
            """,
                (winner_user_id, current_display_name, points_to_add, 1, now_utc),
            )
            logger.info(
                f"User {winner_user_id} ({current_display_name}) added to leaderboard: {points_to_add} points, 1 win."
            )
        conn.commit()
    except sqlite3.Error as e:
        logger.error(
            f"DB error in update_leaderboard for user {winner_user_id}: {e}")
        conn.rollback()
    finally:
        conn.close()


def update_round_robin_player_stats(
    tournament_id: str, user_id: int, username: str, goals_for: int, goals_against: int
):
    """Updates a player's statistics in the round_robin_standings table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO round_robin_standings (tournament_id, user_id, username)
            VALUES (?, ?, ?)
        """,
            (tournament_id, user_id, username),
        )

        wins = 0
        draws = 0
        losses = 0
        points_earned = 0
        if goals_for > goals_against:
            wins = 1
            points_earned = POINTS_FOR_WIN
        elif goals_for == goals_against:
            draws = 1
            points_earned = POINTS_FOR_DRAW
        else:
            losses = 1
            points_earned = POINTS_FOR_LOSS

        goal_difference = goals_for - goals_against

        cursor.execute(
            """
            UPDATE round_robin_standings
            SET
                games_played = games_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + ?,
                points = points + ?
            WHERE tournament_id = ? AND user_id = ?
        """,
            (
                wins,
                draws,
                losses,
                goals_for,
                goals_against,
                goal_difference,
                points_earned,
                tournament_id,
                user_id,
            ),
        )
        conn.commit()
        logger.info(
            f"Updated RR standings for user {user_id} in T_ID {tournament_id}.")
    except sqlite3.Error as e:
        logger.error(
            f"DB error updating RR standings for user {user_id} in T_ID {tournament_id}: {e}"
        )
        conn.rollback()
    finally:
        conn.close()


def get_round_robin_standings(tournament_id: str) -> list:
    """Fetches standings for a Round Robin or Swiss tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT username, user_id, games_played, wins, draws, losses, goals_for, goals_against, goal_difference, points
            FROM round_robin_standings
            WHERE tournament_id = ?
            ORDER BY points DESC, goal_difference DESC, goals_for DESC, username ASC
        """,
            (tournament_id,),
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(
            f"DB get_round_robin_standings for T_ID {tournament_id}: {e}")
        return []
    finally:
        conn.close()


def generate_round_robin_fixtures(players: list) -> list:
    """Generates a round-robin schedule for a given list of players.
    Uses the 'circle' method for even number of players,
    and a modified version for odd number of players (with a dummy player).
    Returns a list of rounds, where each round is a list of matches.
    Each match is a tuple: (player1_data, player2_data).
    """
    n = len(players)
    if n % 2 != 0:
        # Add a dummy player for odd number of players
        players.append({"user_id": None, "username": "BYE"})
        n += 1  # Update n to be even

    schedule = []
    # Create pairs of players
    pairs = list(players)

    # Number of rounds = n - 1
    for i in range(n - 1):
        round_matches = []
        # Pair first player with the last one
        round_matches.append((pairs[0], pairs[n - 1]))

        # Pair remaining players
        for j in range(1, n // 2):
            round_matches.append((pairs[j], pairs[n - 1 - j]))
        schedule.append(round_matches)

        # Rotate players: keep first player fixed, rotate others
        temp = pairs[n - 1]
        for j in range(n - 1, 1, -1):
            pairs[j] = pairs[j - 1]
        pairs[1] = temp

    # If an odd number of actual players, remove BYE matches from schedule
    final_schedule = []
    for round_matches in schedule:
        current_round_actual_matches = []
        for p1, p2 in round_matches:
            # Only include matches where neither player is the BYE dummy
            if p1["user_id"] is not None and p2["user_id"] is not None:
                current_round_actual_matches.append((p1, p2))
            # If one player is BYE, the other gets a bye in this round, so no
            # match is played.
        if current_round_actual_matches:  # Only add rounds that actually have matches
            final_schedule.append(current_round_actual_matches)
    return final_schedule


def get_player_matches(tournament_id: str, user_id: int) -> list:
    """Fetches all matches a player has participated in for a given tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT player1_user_id, player2_user_id
            FROM matches
            WHERE tournament_id = ? AND (player1_user_id = ? OR player2_user_id = ?) AND status = 'completed'
        """,
            (tournament_id, user_id, user_id),
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(
            f"DB get_player_matches for T_ID {tournament_id}, P_ID {user_id}: {e}"
        )
        return []
    finally:
        conn.close()


def has_played_against(
    player1_id: int, player2_id: int, existing_matches: list
) -> bool:
    """Checks if two players have already played against each other based on a list of matches."""
    for match in existing_matches:
        if (
            match["player1_user_id"] == player1_id
            and match["player2_user_id"] == player2_id
        ) or (
            match["player1_user_id"] == player2_id
            and match["player2_user_id"] == player1_id
        ):  # Corrected this line
            return True
    return False


def generate_swiss_round_matches(
    tournament_id: str, round_number: int, registered_players: list
) -> list:
    """Generates matches for a Swiss tournament round."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    # Get current standings to sort players
    standings = get_round_robin_standings(tournament_id)
    # Ensure all registered players are in standings, even if they have 0
    # points
    standing_player_ids = {s["user_id"] for s in standings}
    for player in registered_players:
        if player["user_id"] not in standing_player_ids:
            standings.append(
                {
                    "user_id": player["user_id"],
                    "username": player["username"],
                    "games_played": 0,
                    "wins": 0,
                    "draws": 0,
                    "losses": 0,
                    "goals_for": 0,
                    "goals_against": 0,
                    "goal_difference": 0,
                    "points": 0,
                }
            )

    # Sort players by points, then goal difference, then goals for, then
    # username
    standings.sort(
        key=lambda x: (
            x["points"],
            x["goal_difference"],
            x["goals_for"],
            x["username"],
        ),
        reverse=True,
    )

    players_to_pair = [
        {"user_id": s["user_id"], "username": s["username"]} for s in standings
    ]
    matches_for_round = []
    paired_players_ids = set()

    # Get all previous matches for rematch checking
    all_previous_matches = get_matches_for_tournament(
        tournament_id, match_status="completed", group_id=None
    )  # Only consider completed matches for rematch history

    match_in_round_idx = 0

    # Create a mutable copy of players_to_pair for easier removal
    unpaired_players = list(players_to_pair)

    while unpaired_players:
        p1 = unpaired_players.pop(0)  # Take the top available player

        if p1["user_id"] in paired_players_ids:
            continue  # Already paired

        found_opponent = False
        # Try to find an opponent for p1 from the remaining unpaired players
        for i, p2 in enumerate(unpaired_players):
            if p2["user_id"] in paired_players_ids:
                continue

            # Check for rematches
            if not has_played_against(
                p1["user_id"], p2["user_id"], all_previous_matches
            ):
                match_in_round_idx += 1
                m_dets = {
                    "tournament_id": tournament_id,
                    "round_number": round_number,
                    "match_in_round_index": match_in_round_idx,
                    "player1_user_id": p1["user_id"],
                    "player1_username": p1["username"],
                    "player2_user_id": p2["user_id"],
                    "player2_username": p2["username"],
                    "status": "scheduled",
                    "next_match_id": None,
                    "group_id": None,  # Swiss matches are not in groups
                }
                matches_for_round.append(m_dets)
                paired_players_ids.add(p1["user_id"])
                paired_players_ids.add(p2["user_id"])
                unpaired_players.pop(i)  # Remove p2 from unpaired list
                found_opponent = True
                break

        if not found_opponent and p1["user_id"] is not None:
            # If no opponent found, this player gets a BYE
            match_in_round_idx += 1
            m_dets = {
                "tournament_id": tournament_id,
                "round_number": round_number,
                "match_in_round_index": match_in_round_idx,
                "player1_user_id": p1["user_id"],
                "player1_username": p1["username"],
                "player2_user_id": None,
                "player2_username": "BYE",
                # Player gets a win for the BYE
                "winner_user_id": p1["user_id"],
                "score": "W-L",  # Representing a win by default
                "status": "bye",
                "next_match_id": None,
                "group_id": None,
            }
            matches_for_round.append(m_dets)
            paired_players_ids.add(p1["user_id"])
            # Update standings for the BYE player immediately
            update_round_robin_player_stats(
                tournament_id, p1["user_id"], p1["username"], 1, 0
            )  # 1 goal for, 0 against for a win
            logger.info(
                f"Player {
                    p1['username']} gets a BYE in Swiss round {round_number} for T_ID {tournament_id}."
            )

    return matches_for_round


async def generate_swiss_knockout_bracket(
    context: ContextTypes.DEFAULT_TYPE,
    tournament_id: str,
    tournament_name: str,
    num_qualifiers: int,
):
    """Generates the knockout bracket for a Swiss tournament."""
    logger.info(
        f"Generating Swiss knockout bracket for T_ID {tournament_id} with {num_qualifiers} qualifiers."
    )

    standings = get_round_robin_standings(tournament_id)
    # Filter out BYE players if they exist in standings and ensure only actual
    # players are considered
    actual_players_in_standings = [
        p for p in standings if p["user_id"] is not None]

    if len(actual_players_in_standings) < 2:
        await send_public_announcement(
            context,
            tournament_id,
            escape_markdown_v2(
                f"⚠️ Not enough active players in Swiss tournament *{tournament_name}* to form a knockout bracket\\. Tournament concluded without a champion from knockout\\."
            ),
        )
        update_tournament_status(tournament_id, "completed")
        return

    # Select top N players for knockout
    qualifying_players = actual_players_in_standings[:num_qualifiers]

    if len(qualifying_players) < 2:
        await send_public_announcement(
            context,
            tournament_id,
            escape_markdown_v2(
                f"⚠️ Not enough players qualified from Swiss league stage for *{tournament_name}* to form a knockout bracket\\. Tournament concluded without a champion from knockout\\."
            ),
        )
        update_tournament_status(tournament_id, "completed")
        return

    # Update tournament status to ongoing_knockout
    if not update_tournament_status(tournament_id, "ongoing_knockout"):
        logger.error(
            f"Failed to update T_ID {tournament_id} status to 'ongoing_knockout'."
        )
        await send_public_announcement(
            context,
            tournament_id,
            escape_markdown_v2(
                f"⚠️ Failed to start knockout stage for *{tournament_name}*\\. Please contact admin\\."
            ),
        )
        return

    parts_ko_gen = [
        escape_markdown_v2(
            f"\n🔥 *Knockout Stage Initiated for {tournament_name}!*")
    ]
    parts_ko_gen.append(
        escape_markdown_v2(
            "Top qualifiers from the Swiss league stage will now battle it out in a single-elimination bracket\\."
        )
    )

    num_knockout_players = len(qualifying_players)
    num_rounds_full_bracket = (
        math.ceil(math.log2(num_knockout_players)
                  ) if num_knockout_players > 0 else 0
    )
    full_knockout_bracket_size = (
        2**num_rounds_full_bracket if num_knockout_players > 0 else 0
    )

    knockout_participants_data = list(qualifying_players) + [
        {"user_id": None, "username": "BYE"}
    ] * (full_knockout_bracket_size - num_knockout_players)
    random.shuffle(
        knockout_participants_data
    )  # Shuffle for fairness in knockout seed if not predefined

    current_round_num_ko = 1  # Knockout rounds start from 1
    active_nodes_for_next_ko_round = []
    temp_player_processing_list_ko = list(knockout_participants_data)

    # First round of knockout (from advancing players and BYEs)
    match_in_round_idx_ko = 0
    while temp_player_processing_list_ko:
        p1_data = temp_player_processing_list_ko.pop(0)
        if (
            not temp_player_processing_list_ko
        ):  # Odd number of players left, this one gets a bye
            if p1_data["user_id"] is not None:
                active_nodes_for_next_ko_round.append(
                    {
                        "type": "player",
                        "user_id": p1_data["user_id"],
                        "username": p1_data["username"],
                    }
                )
                parts_ko_gen.append(
                    f"  KO R1: {
                        escape_markdown_v2(
                            p1_data['username'])} gets a BYE into the next stage of bracket building\\."
                )
            break

        p2_data = temp_player_processing_list_ko.pop(0)
        match_in_round_idx_ko += 1

        if p1_data["user_id"] is not None and p2_data["user_id"] is not None:
            m_dets_ko = {
                "tournament_id": tournament_id,
                "round_number": current_round_num_ko,
                "match_in_round_index": match_in_round_idx_ko,
                "player1_user_id": p1_data["user_id"],
                "player1_username": p1_data["username"],
                "player2_user_id": p2_data["user_id"],
                "player2_username": p2_data["username"],
                "status": "scheduled",
                "next_match_id": None,
                "group_id": None,  # No group for knockout matches
            }
            m_id_ko = add_match_to_db(m_dets_ko)
            if m_id_ko:
                active_nodes_for_next_ko_round.append(
                    {"type": "match", "id": m_id_ko})
                parts_ko_gen.append(
                    f"  KO R{current_round_num_ko} M{match_in_round_idx_ko}: {
                        escape_markdown_v2(
                            m_dets_ko['player1_username'])} vs {
                        escape_markdown_v2(
                            m_dets_ko['player2_username'])} \\(ID: `{m_id_ko}`\\)"
                )
                await notify_players_of_match(
                    context,
                    m_id_ko,
                    tournament_id,
                    tournament_name,
                    p1_data["user_id"],
                    p1_data["username"],
                    p2_data["user_id"],
                    p2_data["username"],
                )
        elif p1_data["user_id"] is not None:  # p2 is BYE
            active_nodes_for_next_ko_round.append(
                {
                    "type": "player",
                    "user_id": p1_data["user_id"],
                    "username": p1_data["username"],
                }
            )
            parts_ko_gen.append(
                f"  KO R{current_round_num_ko}: {
                    escape_markdown_v2(
                        p1_data['username'])} gets a BYE \\(vs virtual BYE player\\)\\."
            )
        elif p2_data["user_id"] is not None:  # p1 is BYE
            active_nodes_for_next_ko_round.append(
                {
                    "type": "player",
                    "user_id": p2_data["user_id"],
                    "username": p2_data["username"],
                }
            )
            parts_ko_gen.append(
                f"  KO R{current_round_num_ko}: {
                    escape_markdown_v2(
                        p2_data['username'])} gets a BYE \\(vs virtual BYE player\\)\\."
            )

    # Build subsequent knockout rounds (shell matches)
    current_round_num_ko += 1  # Move to next round
    while len(active_nodes_for_next_ko_round) > 1:
        match_in_idx_shell = 0
        temp_active_shell_nodes = list(active_nodes_for_next_ko_round)
        active_nodes_for_next_ko_round.clear()

        while temp_active_shell_nodes:
            node1_adv = temp_active_shell_nodes.pop(0)
            if not temp_active_shell_nodes:
                active_nodes_for_next_ko_round.append(node1_adv)
                logger.info(
                    f"Node gets bye to next knockout shell round {current_round_num_ko}."
                )
                break

            node2_adv = temp_active_shell_nodes.pop(0)
            match_in_idx_shell += 1
            shell_dets = {
                "tournament_id": tournament_id,
                "round_number": current_round_num_ko,
                "match_in_round_index": match_in_idx_shell,
                "player1_user_id": None,
                "player1_username": None,
                "player2_user_id": None,
                "player2_username": None,
                "status": "pending_players",
                "next_match_id": None,
                "group_id": None,
            }

            if node1_adv["type"] == "player":
                shell_dets["player1_user_id"] = node1_adv["user_id"]
                shell_dets["player1_username"] = node1_adv["username"]
            if node2_adv["type"] == "player":
                if shell_dets["player1_user_id"] is None:
                    shell_dets["player1_user_id"] = node2_adv["user_id"]
                    shell_dets["player1_username"] = node2_adv["username"]
                else:
                    shell_dets["player2_user_id"] = node2_adv["user_id"]
                    shell_dets["player2_username"] = node2_adv["username"]

            # If both players for a shell match are now known (due to BYEs)
            # then schedule it immediately
            if shell_dets["player1_user_id"] and shell_dets["player2_user_id"]:
                shell_dets["status"] = "scheduled"
                parts_ko_gen.append(
                    f"  KO R{current_round_num_ko} M{match_in_idx_shell} \\(Auto\\-Scheduled BYE vs BYE\\): {
                        escape_markdown_v2(
                            shell_dets['player1_username'])} vs {
                        escape_markdown_v2(
                            shell_dets['player2_username'])}"
                )

            new_shell_id = add_match_to_db(shell_dets)
            if not new_shell_id:
                logger.error(
                    f"CRITICAL: Failed to create knockout shell match R{current_round_num_ko}M{match_in_idx_shell}. Tournament {tournament_id} may be inconsistent."
                )
                continue
            active_nodes_for_next_ko_round.append(
                {"type": "match", "id": new_shell_id})

            # Link previous matches to this new shell match
            conn_link = sqlite3.connect(DB_NAME)
            cur_link = conn_link.cursor()
            try:
                if node1_adv["type"] == "match":
                    cur_link.execute(
                        "UPDATE matches SET next_match_id = ? WHERE match_id = ?",
                        (new_shell_id, node1_adv["id"]),
                    )
                if node2_adv["type"] == "match":
                    cur_link.execute(
                        "UPDATE matches SET next_match_id = ? WHERE match_id = ?",
                        (new_shell_id, node2_adv["id"]),
                    )
                conn_link.commit()
            except sqlite3.Error as e_link:
                logger.error(
                    f"Error linking previous knockout matches to shell {new_shell_id}: {e_link}"
                )
            finally:
                conn_link.close()

            if (
                shell_dets["status"] == "scheduled"
            ):  # If this match is now fully determined
                await notify_players_of_match(
                    context,
                    new_shell_id,
                    tournament_id,
                    tournament_name,
                    shell_dets["player1_user_id"],
                    shell_dets["player1_username"],
                    shell_dets["player2_user_id"],
                    shell_dets["player2_username"],
                )
        current_round_num_ko += (
            1  # Advance round number after processing all matches in current shell
        )

    # Final message about knockout stage
    if (
        len(active_nodes_for_next_ko_round) == 1
        and active_nodes_for_next_ko_round[0]["type"] == "match"
    ):
        parts_ko_gen.append(
            escape_markdown_v2(
                f"\nKnockout bracket created successfully! Final Match ID will be: `{
                    active_nodes_for_next_ko_round[0]['id']}`."
            )
        )
    elif not active_nodes_for_next_ko_round and num_knockout_qualifiers > 0:
        parts_ko_gen.append(
            escape_markdown_v2(
                "\n⚠️ Bracket generation completed, but no final match node identified. Check logs."
            )
        )
    elif len(active_nodes_for_next_ko_round) > 1:
        parts_ko_gen.append(
            escape_markdown_v2(
                "\n⚠️ Bracket generation completed with multiple final nodes. This indicates an issue."
            )
        )

    if num_qualifiers > 1:
        parts_ko_gen.append(
            escape_markdown_v2(
                "\nGood luck to all knockout participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
            )
        )
    await send_public_announcement(context, tournament_id, "\n".join(parts_ko_gen))


# --- NEW DATABASE HELPER FUNCTIONS FOR GROUP STAGE & KNOCKOUT ---
def add_group_to_db(tournament_id: str, group_name: str) -> int | None:
    """Adds a new group to the database for a tournament."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO groups_tournament (tournament_id, group_name) VALUES (?, ?)",
            (tournament_id, group_name),
        )
        conn.commit()
        return cursor.lastrowid
    except sqlite3.Error as e:
        logger.error(f"DB add_group_to_db: {e}")
        return None
    finally:
        conn.close()


def add_player_to_group_db(group_id: int, user_id: int, username: str) -> bool:
    """Adds a player to a specific group."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO group_participants (group_id, user_id, username) VALUES (?, ?, ?)",
            (group_id, user_id, username),
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"DB add_player_to_group_db: {e}")
        return False
    finally:
        conn.close()


def get_groups_for_tournament(tournament_id: str) -> list:
    """Fetches all groups associated with a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT group_id, group_name FROM groups_tournament WHERE tournament_id = ? ORDER BY group_name",
            (tournament_id,),
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_groups_for_tournament: {e}")
        return []
    finally:
        conn.close()


def get_players_in_group(group_id: int) -> list:
    """Fetches all players assigned to a specific group."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT user_id, username FROM group_participants WHERE group_id = ?",
            (group_id,),
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_players_in_group: {e}")
        return []
    finally:
        conn.close()


def update_group_stage_player_stats(
    tournament_id: str,
    group_id: int,
    user_id: int,
    username: str,
    goals_for: int,
    goals_against: int,
):
    """Updates a player's statistics in the group_stage_standings table."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO group_stage_standings (tournament_id, group_id, user_id, username)
            VALUES (?, ?, ?, ?)
        """,
            (tournament_id, group_id, user_id, username),
        )

        wins = 0
        draws = 0
        losses = 0
        points_earned = 0
        if goals_for > goals_against:
            wins = 1
            points_earned = POINTS_FOR_WIN
        elif goals_for == goals_against:
            draws = 1
            points_earned = POINTS_FOR_DRAW
        else:
            losses = 1
            points_earned = POINTS_FOR_LOSS

        goal_difference = goals_for - goals_against

        cursor.execute(
            """
            UPDATE group_stage_standings
            SET
                games_played = games_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + ?,
                points = points + ?
            WHERE tournament_id = ? AND group_id = ? AND user_id = ?
        """,
            (
                wins,
                draws,
                losses,
                goals_for,
                goals_against,
                goal_difference,
                points_earned,
                tournament_id,
                group_id,
                user_id,
            ),
        )
        conn.commit()
        logger.info(
            f"Updated group stage standings for user {user_id} in T_ID {tournament_id}, Group {group_id}."
        )
    except sqlite3.Error as e:
        logger.error(
            f"DB error updating group stage standings for user {user_id} in T_ID {tournament_id}, Group {group_id}: {e}"
        )
        conn.rollback()
    finally:
        conn.close()


def get_group_stage_standings(tournament_id: str, group_id: int) -> list:
    """Fetches standings for a specific group in a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT username, user_id, games_played, wins, draws, losses, goals_for, goals_against, goal_difference, points
            FROM group_stage_standings
            WHERE tournament_id = ? AND group_id = ?
            ORDER BY points DESC, goal_difference DESC, goals_for DESC, username ASC
        """,
            (tournament_id, group_id),
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(
            f"DB get_group_stage_standings for T_ID {tournament_id}, G_ID {group_id}: {e}"
        )
        return []
    finally:
        conn.close()


def get_advancing_players_from_groups(tournament_id: str) -> list:
    """Determines and returns players advancing from group stages (top 2 from each group)."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    advancing_players = []
    try:
        groups = get_groups_for_tournament(tournament_id)
        for group in groups:
            group_id = group["group_id"]
            standings = get_group_stage_standings(tournament_id, group_id)
            # Take top 2 from each group
            if len(standings) >= 2:
                advancing_players.append(standings[0])
                advancing_players.append(standings[1])
            elif len(standings) == 1:  # If only one player somehow, they advance
                advancing_players.append(standings[0])
            else:
                logger.warning(
                    f"Tournament {tournament_id}: Group {group_id} has no players in standings."
                )
    except sqlite3.Error as e:
        logger.error(
            f"DB get_advancing_players_from_groups for T_ID {tournament_id}: {e}"
        )
    finally:
        conn.close()
    return advancing_players


def get_matches_for_group(
    tournament_id: str, group_id: int, match_status: str | None = None
) -> list:
    """Fetches matches for a specific group in a tournament."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    matches_list = []
    query = "SELECT * FROM matches WHERE tournament_id = ? AND group_id = ?"
    params = [tournament_id, group_id]
    if match_status:
        query += " AND status = ?"
        params.append(match_status)
    query += " ORDER BY round_number, match_in_round_index"
    try:
        cursor.execute(query, tuple(params))
        matches_list = cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(
            f"DB get_matches_for_group {tournament_id}, group {group_id}: {e}")
    finally:
        conn.close()
    return matches_list


# --- NEW Score Submission Helper Functions ---
def add_score_submission(
    match_id: int, user_id: int, score_p1: int, score_p2: int
) -> bool:
    """Adds a score submission for a match by a specific user."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO score_submissions (match_id, user_id, score_p1, score_p2)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(match_id, user_id) DO UPDATE SET
                score_p1 = EXCLUDED.score_p1,
                score_p2 = EXCLUDED.score_p2,
                submission_time = CURRENT_TIMESTAMP
        """,
            (match_id, user_id, score_p1, score_p2),
        )
        conn.commit()
        logger.info(
            f"Score submission for match {match_id} by user {user_id} recorded."
        )
        return True
    except sqlite3.Error as e:
        logger.error(f"DB add_score_submission: {e}")
        return False
    finally:
        conn.close()


def get_score_submissions_for_match(match_id: int) -> list:
    """Fetches all score submissions for a given match."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT * FROM score_submissions WHERE match_id = ?", (match_id,)
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB get_score_submissions_for_match: {e}")
        return []
    finally:
        conn.close()


def clear_score_submissions_for_match(match_id: int) -> bool:
    """Clears all score submissions for a specific match."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM score_submissions WHERE match_id = ?", (match_id,))
        conn.commit()
        logger.info(f"Cleared score submissions for match {match_id}.")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB clear_score_submissions_for_match: {e}")
        return False
    finally:
        conn.close()


# --- Table Generation Helper Function ---
def generate_league_table(team_data):
    """
    Generates a league table string formatted like the provided image.
    This function produces PLAIN TEXT, no Markdown escaping needed inside it.
    """

    COL_WIDTHS = {
        "rank": 2,
        "team": 15,  # Adjusted for shorter names, can be increased if needed
        "pl": 2,
        "w": 1,
        "d": 1,
        "l": 1,
        "plus_minus": 5,  # For "GF-GA" format
        "gd": 4,
        "pts": 2,
    }

    # Header row
    header = (
        f"{'#':<{COL_WIDTHS['rank']}} "
        f"{'Team':<{COL_WIDTHS['team']}} "
        f"{'Pl':<{COL_WIDTHS['pl']}} "
        f"{'W':<{COL_WIDTHS['w']}} "
        f"{'D':<{COL_WIDTHS['d']}} "
        f"{'L':<{COL_WIDTHS['l']}} "
        f"{'+/-':<{COL_WIDTHS['plus_minus']}} "
        f"{'GD':<{COL_WIDTHS['gd']}} "
        f"{'Pts':<{COL_WIDTHS['pts']}}"
    )

    # Separator line (adjust length based on header)
    separator = "-" * len(header)

    table_lines = [header, separator]

    for team in team_data:
        # Truncate team name if it's too long
        team_name_display = team["team_name"]
        if len(team_name_display) > COL_WIDTHS["team"]:
            team_name_display = team_name_display[:
                                                  COL_WIDTHS["team"] - 3] + "..."

        # Format goal difference with a sign (+ or -)
        gd_display = (
            # Adds a + sign for positive numbers
            f"{team['goal_difference']:+}"
        )

        # Format goals for/against as "GF-GA"
        plus_minus_display = f"{team['goals_for']}-{team['goals_against']}"

        row = (
            f"{str(team['rank']):<{COL_WIDTHS['rank']}} "
            f"{team_name_display:<{COL_WIDTHS['team']}} "
            f"{str(team['played']):<{COL_WIDTHS['pl']}} "
            f"{str(team['wins']):<{COL_WIDTHS['w']}} "
            f"{str(team['draws']):<{COL_WIDTHS['d']}} "
            f"{str(team['losses']):<{COL_WIDTHS['l']}} "
            f"{plus_minus_display:<{COL_WIDTHS['plus_minus']}} "
            f"{gd_display:<{COL_WIDTHS['gd']}} "
            f"{str(team['points']):<{COL_WIDTHS['pts']}}"
        )
        table_lines.append(row)

    return "\n".join(table_lines)


# --- Helper Functions (Public Announcements, Glory Board, Match DMs) ---
async def send_public_announcement(
    context: ContextTypes.DEFAULT_TYPE,
    tournament_id: str,
    message_text: str,
    parse_mode: str = "MarkdownV2",
):
    """Sends a public announcement to the tournament's designated group chat."""
    if not tournament_id:
        logger.warning(
            "send_public_announcement called without tournament_id.")
        return
    tournament_details = get_tournament_details_by_id(tournament_id)
    if not tournament_details:
        logger.warning(
            f"send_public_announcement: Tournament {tournament_id} not found for announcement."
        )
        return
    group_chat_id = tournament_details.get("group_chat_id")
    if group_chat_id:
        try:
            await context.bot.send_message(
                chat_id=group_chat_id, text=message_text, parse_mode=parse_mode
            )
            logger.info(
                f"Sent public announcement to group {group_chat_id} for T_ID {tournament_id}."
            )
        except Forbidden:
            logger.warning(
                f"Bot is forbidden to send messages to group {group_chat_id} for T_ID {tournament_id}."
            )
            creator_id = tournament_details.get("creator_id")
            if creator_id:
                try:
                    t_name_esc = escape_markdown_v2(
                        tournament_details.get("name", "Unknown Tournament")
                    )
                    await context.bot.send_message(
                        creator_id,
                        f"⚠️ I couldn't send an announcement for tournament '{t_name_esc}' to the designated group \\(ID: `{group_chat_id}`\\)\\. "
                        f"Please check if I'm still in the group and have permission to send messages, or re\\-set the group using `/set_announcement_group {tournament_id}` in the correct group\\.",
                        parse_mode="MarkdownV2",
                    )
                except Exception as e_creator_dm:
                    logger.error(
                        f"Failed to DM creator about Forbidden error for group {group_chat_id}: {e_creator_dm}"
                    )
        except Exception as e:
            logger.error(
                f"Failed to send public announcement to group {group_chat_id} for T_ID {tournament_id}: {e}"
            )
    else:
        logger.info(
            f"No announcement group set for T_ID {tournament_id}. Public announcement not sent."
        )


async def send_creator_log(
    context: ContextTypes.DEFAULT_TYPE,
    tournament_id: str,
    message: str,
    parse_mode: str = "MarkdownV2",
):
    """Sends a log message to the tournament creator's private chat."""
    if not tournament_id:
        return

    tournament_details = get_tournament_details_by_id(tournament_id)
    if not tournament_details or not tournament_details.get("creator_id"):
        return

    creator_id = tournament_details["creator_id"]

    try:
        await context.bot.send_message(
            chat_id=creator_id, text=message, parse_mode=parse_mode
        )
        logger.info(
            f"Sent log to creator {creator_id} for T_ID {tournament_id}")
    except Forbidden:
        logger.warning(
            f"Could not send log to creator {creator_id}. Bot may be blocked."
        )
    except Exception as e:
        logger.error(f"Failed to send log to creator {creator_id}: {e}")


async def send_tournament_glory_board(
    context: ContextTypes.DEFAULT_TYPE,
    tournament_details: dict,
    champion_id: int,
    champion_username: str,
):
    """Sends a summary of the concluded tournament, including champion and path to victory."""
    logger.info(
        f"Attempting to send Glory Board for tournament {
            tournament_details['id']}"
    )
    t_id = tournament_details["id"]
    t_name_esc = escape_markdown_v2(tournament_details["name"])
    t_game_esc = escape_markdown_v2(tournament_details.get("game", "N/A"))
    actual_registered_count = get_registration_count(t_id)
    actual_participants_esc = escape_markdown_v2(str(actual_registered_count))
    concluded_date_esc = escape_markdown_v2(
        datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    champion_username_esc = escape_markdown_v2(champion_username)
    t_time_esc = escape_markdown_v2(
        tournament_details.get(
            "tournament_time", "N/A"))
    t_pk_esc = escape_markdown_v2(tournament_details.get("penalties", "N/A"))
    t_et_esc = escape_markdown_v2(tournament_details.get("extra_time", "N/A"))
    t_cond_esc = escape_markdown_v2(
        tournament_details.get(
            "conditions", "None"))

    runner_up_username_esc = escape_markdown_v2("N/A")
    # Determine runner-up based on tournament type
    if tournament_details.get("type") == "Single Elimination" or (
        tournament_details.get("type") == "Swiss"
        and tournament_details.get("status") == "completed"
    ):  # For Swiss, check final KO match
        final_match = get_final_match_details(t_id)
        if final_match:
            p1_id_final = final_match.get("player1_user_id")
            p2_id_final = final_match.get("player2_user_id")
            if p1_id_final == champion_id and p2_id_final:
                runner_up_display_name = (
                    final_match.get("player2_username")
                    or get_player_username_by_id(p2_id_final)
                    or "Runner-Up"
                )
                runner_up_username_esc = escape_markdown_v2(
                    runner_up_display_name)
            elif p2_id_final == champion_id and p1_id_final:
                runner_up_display_name = (
                    final_match.get("player1_username")
                    or get_player_username_by_id(p1_id_final)
                    or "Runner-Up"
                )
                runner_up_username_esc = escape_markdown_v2(
                    runner_up_display_name)
    elif tournament_details.get("type") == "Round Robin":
        conn_rr = sqlite3.connect(DB_NAME)
        conn_rr.row_factory = dict_factory
        cursor_rr = conn_rr.cursor()
        try:
            cursor_rr.execute(
                """
                SELECT username, user_id FROM round_robin_standings
                WHERE tournament_id = ?
                ORDER BY points DESC, goal_difference DESC, goals_for DESC, username ASC
                LIMIT 2
            """,
                (t_id,),
            )
            top_two = cursor_rr.fetchall()
            if len(top_two) > 1 and top_two[0]["user_id"] == champion_id:
                runner_up_username_esc = escape_markdown_v2(
                    top_two[1]["username"] or f"User_{top_two[1]['user_id']}"
                )
        except sqlite3.Error as e:
            logger.error(
                f"Error fetching RR/Swiss standings for glory board: {e}")
        finally:
            conn_rr.close()
    elif tournament_details.get("type") == "Group Stage & Knockout":
        final_ko_match = get_final_match_details(t_id)
        if final_ko_match:
            p1_id_final = final_ko_match.get("player1_user_id")
            p2_id_final = final_ko_match.get("player2_user_id")
            if p1_id_final == champion_id and p2_id_final:
                runner_up_display_name = (
                    final_ko_match.get("player2_username")
                    or get_player_username_by_id(p2_id_final)
                    or "Runner-Up"
                )
                runner_up_username_esc = escape_markdown_v2(
                    runner_up_display_name)
            elif p2_id_final == champion_id and p1_id_final:
                runner_up_display_name = (
                    final_ko_match.get("player1_username")
                    or get_player_username_by_id(p1_id_final)
                    or "Runner-Up"
                )
                runner_up_username_esc = escape_markdown_v2(
                    runner_up_display_name)

    path_to_victory_parts = [
        f"✨ *{escape_markdown_v2('Champion s Path to Victory')}* ✨"
    ]
    if tournament_details.get("type") in [
        "Single Elimination",
        "Group Stage & Knockout",
        "Swiss",
    ]:  # Include Swiss for KO path
        champion_matches = get_matches_won_by_player(t_id, champion_id)

        # For GS&KO and Swiss, filter for knockout matches only (group_id is
        # NULL for KO matches)
        if tournament_details.get("type") in [
                "Group Stage & Knockout", "Swiss"]:
            champion_matches = [
                m for m in champion_matches if m.get("group_id") is None
            ]

        if champion_matches:
            for match_won in champion_matches:
                opponent_id = None
                opponent_display_name = "Opponent"
                if match_won.get("player1_user_id") == champion_id:
                    opponent_id = match_won.get("player2_user_id")
                    opponent_display_name = match_won.get("player2_username") or (
                        get_player_username_by_id(opponent_id)
                        if opponent_id
                        else "Opponent"
                    )
                else:
                    opponent_id = match_won.get("player1_user_id")
                    opponent_display_name = match_won.get("player1_username") or (
                        get_player_username_by_id(opponent_id)
                        if opponent_id
                        else "Opponent"
                    )

                opponent_username_esc = escape_markdown_v2(
                    opponent_display_name)
                score_esc = escape_markdown_v2(match_won.get("score", "N/A"))

                round_prefix = "R"
                if (
                    tournament_details.get("type") == "Group Stage & Knockout"
                    and match_won.get("group_id") is None
                ):
                    round_prefix = "KO R"
                elif (
                    tournament_details.get("type") == "Swiss"
                    and match_won.get("group_id") is None
                ):
                    # For Swiss, if it's a KO match, it's a KO round. If it's a Swiss league match, it's a Swiss round.
                    # This logic assumes KO matches for Swiss will have round_number > num_swiss_rounds or a different indicator.
                    # For simplicity, if group_id is None, it's a KO match in
                    # Swiss context.
                    if match_won["round_number"] > tournament_details.get(
                        "num_swiss_rounds", 0
                    ):
                        round_prefix = "KO R"
                    else:
                        round_prefix = (
                            "Swiss R"  # This would be for the league phase matches
                        )

                round_num_esc = escape_markdown_v2(
                    str(match_won["round_number"]))

                path_to_victory_parts.append(
                    f"  {round_prefix}{round_num_esc}: Defeated {opponent_username_esc} \\(Score: {score_esc}\\)"
                )
        else:
            path_to_victory_parts.append(
                escape_markdown_v2(
                    "  An incredible undefeated run or bye to victory!")
            )
    elif tournament_details.get("type") == "Round Robin":
        # For Round Robin, show overall stats for the champion
        conn_rr = sqlite3.connect(DB_NAME)
        conn_rr.row_factory = dict_factory
        cursor_rr = conn_rr.cursor()
        try:
            cursor_rr.execute(
                """
                SELECT games_played, wins, draws, losses, goals_for, goals_against, goal_difference, points
                FROM round_robin_standings
                WHERE tournament_id = ? AND user_id = ?
            """,
                (t_id, champion_id),
            )
            champ_stats = cursor_rr.fetchone()
            if champ_stats:
                path_to_victory_parts.append(
                    escape_markdown_v2("  Season Statistics:"))
                path_to_victory_parts.append(
                    escape_markdown_v2(
                        f"    Games Played: {champ_stats['games_played']}"
                    )
                )
                path_to_victory_parts.append(
                    escape_markdown_v2(
                        f"    Wins: {
                            champ_stats['wins']}, Draws: {
                            champ_stats['draws']}, Losses: {
                            champ_stats['losses']}"
                    )
                )
                path_to_victory_parts.append(
                    escape_markdown_v2(
                        f"    Goals For: {
                            champ_stats['goals_for']}, Goals Against: {
                            champ_stats['goals_against']}"
                    )
                )
                path_to_victory_parts.append(
                    escape_markdown_v2(
                        f"    Goal Difference: {
                            champ_stats['goal_difference']}, Total Points: {
                            champ_stats['points']}"
                    )
                )
            else:
                path_to_victory_parts.append(
                    escape_markdown_v2(
                        f"  Dominated the {
                            tournament_details.get(
                                'type', '')} tournament!"
                    )
                )
        except sqlite3.Error as e:
            logger.error(
                f"Error fetching champion RR/Swiss stats for glory board: {e}")
            path_to_victory_parts.append(
                escape_markdown_v2(
                    f"  Dominated the {
                        tournament_details.get(
                            'type', '')} tournament!"
                )
            )
        finally:
            conn_rr.close()

    separator = escape_markdown_v2("----------------------------------------")
    glory_board_caption_parts = [
        f"🎉🏆 *Tournament Concluded: {t_name_esc}* 🏆🎉\n",
        f"🥇   *C H A M P I O N* 🥇",
        f"      *{champion_username_esc}*",
        separator,
    ]
    if runner_up_username_esc != escape_markdown_v2("N/A"):
        glory_board_caption_parts.append(
            f"\n🥈 Runner\\-Up: {runner_up_username_esc}")

    glory_board_caption_parts.append(f"\n🎮 Game: {t_game_esc}")
    glory_board_caption_parts.append(
        f"👥 Participants: {actual_participants_esc}")

    glory_board_caption_parts.extend(
        [
            "\n" + "\n".join(path_to_victory_parts),
            "\n" + separator + "\n",
            escape_markdown_v2("Huge congratulations to all participants! 👏"),
        ]
    )

    full_caption = "\n".join(
        glory_board_caption_parts
    )  # Use the correctly defined list

    # --- Sending the message as a photo with a caption ---
    trophy_image_url = (
        "https://pixabay.com/illustrations/ai-generated-trophy-cup-award-8248622/"
    )

    # Send to the creator
    creator_id = tournament_details.get("creator_id")
    if creator_id:
        try:
            await context.bot.send_photo(
                chat_id=creator_id,
                photo=trophy_image_url,
                caption=full_caption,
                parse_mode="MarkdownV2",
            )
            logger.info(
                f"Glory Board photo sent to creator {creator_id} for T_ID {t_id}"
            )
        except Exception as e:
            logger.error(
                f"Failed to send Glory Board photo to creator {creator_id}: {e}"
            )


async def notify_players_of_match(
    context: ContextTypes.DEFAULT_TYPE,
    match_id: int,
    tournament_id: str,
    tournament_name: str,
    player1_id: int,
    player1_username: str,
    player2_id: int,
    player2_username: str,
):
    """Sends a direct message to players about their upcoming match."""
    logger.info(
        f"Notifying players for match {match_id} in tournament '{tournament_name}' ({tournament_id})"
    )
    # --- ADDED: Send log to creator about the new fixture ---
    p1_name_esc = escape_markdown_v2(player1_username)
    p2_name_esc = escape_markdown_v2(player2_username)
    t_name_esc = escape_markdown_v2(tournament_name)
    log_message = (
        f"🗓️ *New Fixture Scheduled* in '{t_name_esc}'\n"
        f"   Match ID: `{match_id}`\n"
        f"   Fixture: {p1_name_esc} vs {p2_name_esc}"
    )
    await send_creator_log(context, tournament_id, log_message)
    # --- END OF LOG ---
    t_name_esc = escape_markdown_v2(tournament_name)
    match_id_esc = escape_markdown_v2(str(match_id))
    p1_mention = f"[{escape_markdown_v2(player1_username)}](tg://user?id={player1_id})"
    p2_mention = f"[{escape_markdown_v2(player2_username)}](tg://user?id={player2_id})"

    common_message_part = (
        f"🆔 Match ID: `{match_id_esc}`\n\n"
        f"👉 Please coordinate with your opponent to play the match\\. \n"
        f"📝 Report score using: `/report_score {match_id_esc} <your_score> <opponent_score>`\n"
        f"Good luck\\!"
    )

    msg_to_p1 = (
        f"📢 Your match in tournament '{t_name_esc}' is scheduled\\!\n\n"
        f"⚔️ **You \\({p1_mention}\\) vs {p2_mention}**\n"
        f"{common_message_part}"
    )
    msg_to_p2 = (
        f"📢 Your match in tournament '{t_name_esc}' is scheduled\\!\n\n"
        f"⚔️ **You \\({p2_mention}\\) vs {p1_mention}**\n"
        f"{common_message_part}"
    )

    for player_id, message in [
            (player1_id, msg_to_p1), (player2_id, msg_to_p2)]:
        try:
            await context.bot.send_message(player_id, message, parse_mode="MarkdownV2")
            logger.info(
                f"Sent match notification DM to P_ID {player_id} for match {match_id}"
            )
        except Forbidden:
            logger.warning(
                f"Could not DM P_ID {player_id} for match {match_id}. Bot blocked or chat not started."
            )
            tournament_details = get_tournament_details_by_id(tournament_id)
            if tournament_details:
                player_display_name_for_announcement = (
                    player1_username if player_id == player1_id else player2_username
                )
                player_mention_for_announcement = f"[{escape_markdown_v2(player_display_name_for_announcement)}](tg://user?id={player_id})"
                await send_public_announcement(
                    context,
                    tournament_id,
                    f"⚠️ Could not notify {player_mention_for_announcement} via DM for match `{match_id_esc}`\\. "
                    f"Please ensure they have started a chat with the bot and unblocked it\\.",
                )
        except Exception as e:
            logger.error(
                f"Error DMing P_ID {player_id} for match {match_id}: {e}")


async def update_match_score_and_progress(
    context: ContextTypes.DEFAULT_TYPE,
    match_id: int,
    score_str: str,
    reporting_user_id: int,
    winner_user_id: int | None,
    new_status: str = "completed",
) -> bool:
    """Updates match score and status, and progresses the tournament based on type."""
    logger.debug(
        f"Updating match {match_id}. Score: {score_str}, Winner ID: {winner_user_id}, Status: {new_status}"
    )
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        # Update the match first
        cursor.execute(
            "UPDATE matches SET score = ?, winner_user_id = ?, status = ? WHERE match_id = ?",
            (score_str, winner_user_id, new_status, match_id),
        )
        conn.commit()

        if cursor.rowcount == 0:
            logger.warning(f"No rows updated for match {match_id}. Match might not exist.")
            return False

        # Now, fetch details to progress the tournament
        current_match_details = get_match_details_by_match_id(match_id)
        if not current_match_details:
            logger.error(f"Failed to fetch details for updated match {match_id}")
            return True

        tournament = get_tournament_details_by_id(current_match_details["tournament_id"])
        if not tournament:
            logger.error(f"Failed to fetch T_details for match {match_id}")
            return True

        t_id = tournament["id"]
        t_name_esc = escape_markdown_v2(tournament["name"])

        # --- Creator Log (Unchanged) ---
        if new_status == "completed":
            p1_name = escape_markdown_v2(current_match_details["player1_username"])
            p2_name = escape_markdown_v2(current_match_details["player2_username"])
            score_esc = escape_markdown_v2(score_str)
            log_message = (
                f"✅ *Match Completed* in '{t_name_esc}'\n"
                f"   Match ID: `{match_id}`\n"
                f"   Fixture: {p1_name} vs {p2_name}\n"
                f"   Final Score: *{score_esc}*"
            )
            await send_creator_log(context, t_id, log_message)

        # --- Global Stats Update (Unchanged) ---
        if new_status == "completed" and winner_user_id is not None:
            p1_id = current_match_details["player1_user_id"]
            p2_id = current_match_details["player2_user_id"]
            update_global_stats_for_players(p1_id, current_match_details["player1_username"], is_winner=(p1_id == winner_user_id))
            update_global_stats_for_players(p2_id, current_match_details["player2_username"], is_winner=(p2_id == winner_user_id))

        p1_score, p2_score = map(int, score_str.split("-"))
        player1_id_match = current_match_details["player1_user_id"]
        player2_id_match = current_match_details["player2_user_id"]

        # --- START OF LOGIC RESTRUCTURE AND FIX ---

        # Determine if the current match is a knockout match (applies to SE, GS&KO, and Swiss KO)
        is_knockout_match = (
            tournament["type"] == "Single Elimination" or
            (tournament["type"] == "Group Stage & Knockout" and current_match_details.get("group_id") is None) or
            tournament.get("status") == "ongoing_knockout"
        )

        # --- KNOCKOUT PROGRESSION LOGIC (FOR ALL APPLICABLE FORMATS) ---
        if is_knockout_match and new_status == "completed" and winner_user_id:
            winner_display_name = get_player_username_by_id(winner_user_id)
            next_match_id = current_match_details.get("next_match_id")

            if next_match_id:  # Winner advances to the next match
                next_match_details = get_match_details_by_match_id(next_match_id)
                if not next_match_details:
                    logger.error(f"CRITICAL: next_match_id {next_match_id} not found!")
                    return True

                # Place winner in the next available slot of the next match
                if not next_match_details.get("player1_user_id"):
                    cursor.execute("UPDATE matches SET player1_user_id = ?, player1_username = ? WHERE match_id = ?",
                                   (winner_user_id, winner_display_name, next_match_id))
                else:
                    cursor.execute("UPDATE matches SET player2_user_id = ?, player2_username = ? WHERE match_id = ?",
                                   (winner_user_id, winner_display_name, next_match_id))
                conn.commit()

                # Check if the next match is now ready to be scheduled
                updated_next_match = get_match_details_by_match_id(next_match_id)
                if updated_next_match and updated_next_match.get("player1_user_id") and updated_next_match.get("player2_user_id"):
                    cursor.execute("UPDATE matches SET status = 'scheduled' WHERE match_id = ?", (next_match_id,))
                    conn.commit()
                    logger.info(f"Next match {next_match_id} is scheduled.")
                    await notify_players_of_match(
                        context,
                        match_id=next_match_id,
                        tournament_id=t_id,
                        tournament_name=tournament["name"],
                        player1_id=updated_next_match["player1_user_id"],
                        player1_username=updated_next_match["player1_username"],
                        player2_id=updated_next_match["player2_user_id"],
                        player2_username=updated_next_match["player2_username"],
                    )
            else:  # This was the FINAL match
                logger.info(f"Tournament '{tournament['name']}' concluded. Winner: {winner_display_name}")
                if update_tournament_status(t_id, "completed", winner_user_id, winner_display_name):
                    award_achievement(winner_user_id, 'TOURNEY_CHAMPION', tournament_id=t_id)
                    updated_tournament_details = get_tournament_details_by_id(t_id)
                    if updated_tournament_details:
                        winner_username_esc_comp = escape_markdown_v2(winner_display_name)
                        completion_message = f"🏆 Tournament *{t_name_esc}* has concluded\\!\nCongratulations to the champion: *{winner_username_esc_comp}* 🥳"
                        await send_public_announcement(context, t_id, completion_message)
                        update_leaderboard(winner_user_id, winner_display_name)
                        await send_tournament_glory_board(context, updated_tournament_details, winner_user_id, winner_display_name)
        
        # --- LEAGUE/GROUP STAGE PROGRESSION LOGIC ---
        elif not is_knockout_match and new_status == "completed":
            if tournament["type"] in ["Round Robin", "Swiss"]:
                # Update standings for Player 1 & 2
                update_round_robin_player_stats(t_id, player1_id_match, current_match_details["player1_username"], p1_score, p2_score)
                update_round_robin_player_stats(t_id, player2_id_match, current_match_details["player2_username"], p2_score, p1_score)

                # Check if all matches for the current Swiss round are completed
                if tournament["type"] == "Swiss":
                    current_swiss_round = tournament.get("current_swiss_round", 0)
                    num_swiss_rounds = tournament.get("num_swiss_rounds", 0)
                    
                    remaining_matches = get_matches_for_tournament(t_id, "scheduled", round_number=current_swiss_round)
                    if not remaining_matches: # Round is over
                        logger.info(f"All matches for Swiss T_ID {t_id} Round {current_swiss_round} are complete.")
                        
                        if current_swiss_round < num_swiss_rounds:
                             await send_public_announcement(
                                context, t_id,
                                escape_markdown_v2(f"All matches for Round {current_swiss_round} of *{tournament['name']}* are complete! The creator can now generate the next round using `/advance_swiss_round {t_id}`.")
                            )
                        else: # All Swiss rounds are over, time to check for knockout or end
                            logger.info(f"All Swiss rounds completed for T_ID {t_id}.")
                            swiss_ko_qualifiers = tournament.get("swiss_knockout_qualifiers", 0)
                            if swiss_ko_qualifiers and swiss_ko_qualifiers >= 2:
                                await send_public_announcement(
                                    context, t_id,
                                    escape_markdown_v2(f"All Swiss rounds for *{tournament['name']}* are complete! Generating the knockout stage...")
                                )
                                await generate_swiss_knockout_bracket(context, t_id, tournament["name"], swiss_ko_qualifiers)
                            else: # No knockout, determine winner from standings
                                # (This part of your original code was correct)
                                final_winner = get_round_robin_standings(t_id)
                                if final_winner:
                                    winner_details = final_winner[0]
                                    update_tournament_status(t_id, "completed", winner_details['user_id'], winner_details['username'])
                                    # ... (add glory board etc. here)
                                else:
                                    update_tournament_status(t_id, "completed")
            
            # --- (Other tournament type logic like Group Stage can go here) ---
            # This section is simplified for clarity, assuming you will add your GS logic back in
            elif tournament["type"] == "Group Stage & Knockout":
                 # ... your logic for updating group standings ...
                 # ... your logic for checking if all group matches are over and then calling the knockout generator ...
                 pass


        # --- END OF LOGIC RESTRUCTURE AND FIX ---

        return True # Return success
    except sqlite3.Error as e:
        logger.error(f"DB error in update_match_score_and_progress for match {match_id}: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()


# --- Conversation States & Command Handlers ---
(
    ASK_TOURNAMENT_NAME,
    ASK_GAME_NAME,
    ASK_PARTICIPANT_COUNT,
    ASK_TOURNAMENT_TYPE,
    ASK_NUM_GROUPS,
    ASK_SWISS_ROUNDS,
    ASK_SWISS_KNOCKOUT_QUALIFIERS,
    ASK_TOURNAMENT_TIME,
    ASK_PENALTIES,
    ASK_EXTRA_TIME,
    ASK_CONDITIONS,
    CONFIRM_SAVE_TOURNAMENT,
) = range(
    12
)  # Added ASK_SWISS_KNOCKOUT_QUALIFIERS


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and main menu. Now admin-only in groups."""
    
    # 1. Get essential context objects
    chat = update.effective_chat
    user = update.effective_user

    # 2. NEW: Admin-Only Check for Groups
    if chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            admin_ids = [admin.user.id for admin in admins]
            if user.id not in admin_ids:
                logger.info(f"Ignoring /start from non-admin {user.id} in group {chat.id}")
                return # Silently ignore non-admins in groups
        except Exception as e:
            logger.error(f"Failed to check admin status for /start in group {chat.id}: {e}")
            # If the bot can't check admins (e.g., not an admin itself), it will let the command pass for safety.

    # 3. Handle conversation cancellation
    if 'in_conversation' in context.user_data and context.user_data['in_conversation']:
        await update.message.reply_text(
            escape_markdown_v2("It seems you were in the middle of something. Let's start over."),
            parse_mode='MarkdownV2'
        )
        context.user_data.clear()

    # 4. Define the main menu keyboard
    keyboard = [
        [InlineKeyboardButton("🆕 Create Tournament", callback_data="create_tournament")],
        [InlineKeyboardButton("🏆 View Tournaments", callback_data="view_tournaments")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # 5. Send the final welcome message
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! Tournament bot. Options:",
        reply_markup=reply_markup
    )


async def award_badge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows a creator to manually award a custom badge to a player."""
    creator = update.effective_user
    
    if not update.message.reply_to_message:
        await update.message.reply_text("<b>How to use:</b> Reply to a user's message and type `/award_badge <Tournament_ID> \"<Badge Text>\"`", parse_mode='HTML')
        return

    try:
        # Use shlex to handle the quotes in the badge text
        command, tournament_id, badge_text = shlex.split(update.message.text)
    except ValueError:
        await update.message.reply_text('Invalid format. Usage:\n`/award_badge <ID> "Your custom badge text"`', parse_mode='Markdown')
        return
        
    player_to_award = update.message.reply_to_message.from_user

    tournament = get_tournament_details_by_id(tournament_id)
    if not tournament or tournament['creator_id'] != creator.id:
        await update.message.reply_text("You can only award badges for tournaments you created.")
        return

    # Add a custom emoji for the badge text
    full_badge_text = f"🎖️ {badge_text}"
    
    # Award the achievement with a generic code and the custom description
    award_achievement(player_to_award.id, 'CUSTOM', tournament_id=tournament_id, description=full_badge_text)

    await update.message.reply_text(f"✅ Successfully awarded the badge '{badge_text}' to {player_to_award.full_name}.")


async def add_player_command(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows a tournament creator to manually add a player to a pending tournament."""
    creator = update.effective_user

    # 1. Check for correct usage
    if not update.message.reply_to_message:
        await update.message.reply_text("<b>How to use:</b> Reply to a message from the user you want to add and type `/add_player <Tournament_ID>`.", parse_mode='HTML')
        return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text("Usage: /add_player <Tournament_ID>")
        return

    tournament_id = context.args[0]
    player_to_add = update.message.reply_to_message.from_user

    if player_to_add.is_bot:
        await update.message.reply_text("You cannot add a bot to a tournament.")
        return

    # 2. Permission and Tournament Status Checks
    tournament = get_tournament_details_by_id(tournament_id)
    if not tournament:
        await update.message.reply_text(f"Tournament with ID '{tournament_id}' not found.")
        return

    if tournament['creator_id'] != creator.id:
        await update.message.reply_text("Only the creator of this tournament can manually add players.")
        return

    if tournament['status'] != 'pending':
        await update.message.reply_text(f"You can only add players to a tournament that is 'pending'. This tournament's status is '{tournament['status']}'.")
        return

    # 3. Add the player to the database
    player_display_name = player_to_add.full_name or f"User_{player_to_add.id}"
    success = add_registration_to_db(
        tournament_id,
        player_to_add.id,
        player_display_name)

    # 4. Report back to the creator with properly escaped messages
    t_name_esc = escape_markdown_v2(tournament['name'])
    player_name_esc = escape_markdown_v2(player_display_name)

    if success:
        # CORRECTED: Added \. at the end of the sentence.
        message = f"✅ Player *{player_name_esc}* has been successfully added to tournament '{t_name_esc}'\\."

        reg_count = get_registration_count(tournament_id)
        max_p = tournament.get('participants', 0)
        if reg_count > max_p:
            # CORRECTED: Escaped the parentheses and the period.
            message += f"\n\n⚠️ *Warning:* This tournament is now over capacity \\({reg_count}/{max_p}\\)\\."

        await update.message.reply_text(message, parse_mode='MarkdownV2')
    else:
        # CORRECTED: Added \. at the end of the sentence.
        await update.message.reply_text(f"ℹ️ Player *{player_name_esc}* is already registered for this tournament\\.", parse_mode='MarkdownV2')


async def match_history_command(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for viewing a user's match history."""
    user = update.effective_user
    # For now, this command only shows the user's own history.
    target_user_id = user.id

    await send_match_history_page(update, context, target_user_id, page=1)


async def match_history_callback(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the pagination buttons for match history, with a permission check."""
    query = update.callback_query

    # Extract target user and page number from callback_data
    # 'mh_page_{user_id}_{page}'
    try:
        _, _, target_user_id_str, page_str = query.data.split('_')
        target_user_id = int(target_user_id_str)
        page = int(page_str)
    except (ValueError, IndexError):
        await query.answer("Error processing page request.", show_alert=True)
        return

    # --- NEW PERMISSION CHECK ---
    # Check if the person who clicked the button is the person whose history
    # is being shown.
    user_who_clicked = query.from_user
    if user_who_clicked.id != target_user_id:
        # If not, show them a private alert and do nothing else.
        await query.answer("You can only navigate your own match history menu.", show_alert=True)
        return
    # --- END OF CHECK ---

    # If the check passes, answer the query to unfreeze the button and proceed
    await query.answer()
    await send_match_history_page(update, context, target_user_id, page=page, is_callback=True)


async def send_match_history_page(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  target_user_id: int, page: int, is_callback: bool = False):
    """A helper function to send or edit a specific page of the match history, with proper escaping."""
    user = update.effective_user

    matches, total_matches = get_match_history_from_db(
        target_user_id, page=page, limit=5)

    if total_matches == 0:
        message_text = "You have no completed matches in your history."
        if is_callback:
            await update.callback_query.edit_message_text(message_text)
        else:
            await update.message.reply_text(message_text)
        return

    # Build the message content
    user_name_esc = escape_markdown_v2(user.full_name)
    # Escaped parentheses
    message_parts = [
        f"📜 *Match History for {user_name_esc}* \\(Page {page}\\)"]

    for match in matches:
        # Determine opponent and outcome from the user's perspective
        if match['player1_user_id'] == target_user_id:
            opponent_name = match['player2_username']
            score = match['score']
        else:
            opponent_name = match['player1_username']
            # Safely split score
            try:
                p2_score, p1_score = match['score'].split('-')
                score = f"{p1_score}-{p2_score}"  # Reverse score
            except (ValueError, AttributeError):
                # Fallback if score is not in 'X-Y' format
                score = match['score']

        if match['winner_user_id'] == target_user_id:
            outcome = "✅ Win"
        elif match['winner_user_id'] is None:
            outcome = "🤝 Draw"
        else:
            outcome = "❌ Loss"

        # Safely handle old matches with no date and escape all parts
        if match['created_at']:
            match_date = datetime.fromisoformat(
                match['created_at']).strftime('%Y-%m-%d')
            match_date_esc = escape_markdown_v2(match_date)
        else:
            match_date_esc = "Old Match"

        opponent_name_esc = escape_markdown_v2(opponent_name or "Unknown")
        score_esc = escape_markdown_v2(score or "N/A")
        tournament_name_esc = escape_markdown_v2(match['tournament_name'])

        message_parts.append(
            # Escaped parentheses
            f"\n`{match_date_esc}`: {outcome} vs *{opponent_name_esc}* \\({score_esc}\\)\n"
            f"  _in tournament: {tournament_name_esc}_"
        )

    # Build pagination buttons
    buttons = []
    row = []
    total_pages = math.ceil(total_matches / 5)

    if page > 1:
        row.append(
            InlineKeyboardButton(
                "⬅️ Previous",
                callback_data=f"mh_page_{target_user_id}_{
                    page - 1}"))

    # A non-clickable button showing the page number
    row.append(
        InlineKeyboardButton(
            f"Page {page}/{total_pages}",
            callback_data="mh_noop"))

    if page < total_pages:
        row.append(
            InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"mh_page_{target_user_id}_{
                    page + 1}"))

    if row:  # Only add the row if there are buttons
        buttons.append(row)

    reply_markup = InlineKeyboardMarkup(buttons) if buttons else None

    # Send or edit the message
    final_message = "\n".join(message_parts)
    if is_callback:
        await update.callback_query.edit_message_text(final_message, parse_mode='MarkdownV2', reply_markup=reply_markup)
    else:
        await update.message.reply_text(final_message, parse_mode='MarkdownV2', reply_markup=reply_markup)


async def view_tournaments_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a list of tournaments. Now admin-only in groups."""

    # Get essential objects
    chat = update.effective_chat
    user = update.effective_user

    # Admin-Only Check (only for the typed command)
    if update.message and chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            if user.id not in [admin.user.id for admin in admins]:
                logger.info(f"Ignoring /view_tournaments from non-admin {user.id} in group {chat.id}")
                return
        except Exception as e:
            logger.error(f"Failed to check admin status for /view_tournaments: {e}")

    query = update.callback_query
    if query:
        await query.answer()

    user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id

    tournaments = []
    title = ""

    if chat_type in ["group", "supergroup"]:
        title = "🏆 Tournaments for this Group"
        tournaments = get_tournaments_from_db(limit=15, group_chat_id=chat_id)
    else:
        title = "🏆 Your Created Tournaments"
        tournaments = get_tournaments_from_db(limit=10, creator_id=user_id)

    msg_parts = [escape_markdown_v2(title)]
    kb_buttons = []

    if not tournaments:
        if chat_type in ["group", "supergroup"]:
            msg_parts.append(
                escape_markdown_v2(
                    "\nNo tournaments have been linked to this group. The creator can use /set_announcement_group <ID> here."
                )
            )
        else:
            msg_parts.append(
                escape_markdown_v2(
                    "\nNo tournaments found created by you. Use /create to make one."
                )
            )
        reply_markup = None
    else:
        pending_tournaments = [
            t for t in tournaments if t.get("status") == "pending"]
        other_tournaments = [
            t
            for t in tournaments
            if t.get("status") in ["ongoing", "completed", "ongoing_knockout"]
        ]

        if pending_tournaments:
            msg_parts.append(
                escape_markdown_v2("\n*📝 Pending & Open for Registration:*")
            )
            for t in pending_tournaments:
                n_esc, g_esc = escape_markdown_v2(
                    t.get("name", "?")
                ), escape_markdown_v2(t.get("game", "?"))
                t_id, reg_c, max_p = (
                    t.get("id", "?"),
                    get_registration_count(t.get("id", "?")),
                    t.get("participants", 0),
                )
                t_info = f"\n🔹 *{n_esc}* \\({g_esc}\\)\n   Reg: {reg_c}/{max_p}, ID: `{t_id}`"
                msg_parts.append(t_info)
                if reg_c < max_p:
                    kb_buttons.append(
                        [
                            InlineKeyboardButton(
                                f"✅ Join '{n_esc[:20]}'",
                                callback_data=f"join_tournament_{t_id}",
                            )
                        ]
                    )

        if other_tournaments:
            msg_parts.append(escape_markdown_v2(
                "\n\n*▶️ Ongoing & Completed:*"))
            for t in other_tournaments:
                n_esc, stat_esc = escape_markdown_v2(
                    t.get("name", "?")
                ), escape_markdown_v2(t.get("status", "?"))
                t_id = t.get("id", "?")
                t_info = f"\n🔹 *{n_esc}*\n   Status: _{stat_esc}_, ID: `{t_id}`"
                if t.get("status") == "completed" and t.get("winner_username"):
                    t_info += (
                        f"\n   🏆 Winner: *{
                            escape_markdown_v2(
                                t['winner_username'])}*"
                    )
                msg_parts.append(t_info)
                action_button_text = "View Matches/Standings"
                kb_buttons.append(
                    [
                        InlineKeyboardButton(
                            f"👀 {action_button_text} '{n_esc[:15]}'",
                            callback_data=f"view_matches_cmd_{t_id}",
                        )
                    ]
                )

        for t in tournaments:
            if (
                t.get("type") == "Swiss"
                and t.get("status") == "ongoing"
                and t.get("creator_id") == user_id
            ):
                current_swiss_round = t.get("current_swiss_round", 0)
                num_swiss_rounds = t.get("num_swiss_rounds", 0)
                if current_swiss_round < num_swiss_rounds:
                    remaining_matches_in_current_round = get_matches_for_tournament(
                        t["id"],
                        match_status="scheduled",
                        round_number=current_swiss_round,
                        group_id=None,
                    )
                    if not remaining_matches_in_current_round:
                        kb_buttons.append(
                            [
                                InlineKeyboardButton(
                                    f"➡️ Advance Swiss R{
                                        current_swiss_round + 1}",
                                    callback_data=f"advance_swiss_round_{
                                        t['id']}",
                                )
                            ]
                        )

        reply_markup = InlineKeyboardMarkup(kb_buttons) if kb_buttons else None

    full_msg = "\n".join(msg_parts)
    edit_method = query.edit_message_text if query else None
    send_method = (
        update.effective_message.reply_text if update.effective_message else None
    )

    if edit_method:
        try:
            await edit_method(
                text=full_msg, parse_mode="MarkdownV2", reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Edit failed in view_tournaments: {e}")
            if send_method:
                await send_method(
                    text=full_msg, parse_mode="MarkdownV2", reply_markup=reply_markup
                )
    elif send_method:
        await send_method(
            text=full_msg, parse_mode="MarkdownV2", reply_markup=reply_markup
        )


async def handle_join_tournament(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handles a user's request to join a tournament."""
    query = update.callback_query
    user = update.effective_user
    await query.answer("Processing...")

    t_id = query.data.split("join_tournament_")[1]
    display_name = user.username if user.username else user.first_name
    if not display_name:
        display_name = f"User_{user.id}"

    t = get_tournament_details_by_id(t_id)
    msg_raw = ""

    if not t:
        msg_raw = "Tournament not found."
    elif t["status"] != "pending":
        msg_raw = f"Registration for '{
            t['name']}' is closed (Status: {
            t['status']})."
    elif is_user_registered(t_id, user.id):
        msg_raw = f"You are already registered for '{t['name']}'."
    elif get_registration_count(t_id) >= t["participants"]:
        msg_raw = f"Sorry, '{t['name']}' is full."
    else:
        if add_registration_to_db(t_id, user.id, display_name):
            msg_raw = f"✅ Successfully joined '{t['name']}'! Good luck!"
            # --- ADDED: Send log to creator ---
            t_name_esc = escape_markdown_v2(t["name"])
            user_name_esc = escape_markdown_v2(display_name)
            reg_count = get_registration_count(t_id)
            max_p = t["participants"]
            log_message = (
                f"👤 *New Registration* for '{t_name_esc}'\n"
                f"   Player: {user_name_esc}\n"
                f"   Total: {reg_count}/{max_p}"
            )
            await send_creator_log(context, t_id, log_message)
            # --- END OF LOG ---
        else:
            msg_raw = f"⚠️ Could not join '{t['name']}'. An error occurred."

    if msg_raw:
        try:
            await context.bot.send_message(
                user.id, escape_markdown_v2(msg_raw), parse_mode="MarkdownV2"
            )
            await query.answer("Done! Check your DMs.", show_alert=False)
        except Forbidden:
            alert_msg = (
                msg_raw.replace("✅", "").strip()
                if "Successfully joined" in msg_raw
                else "Please start a chat with me directly for DMs and try again."
            )
            await query.answer(alert_msg, show_alert=True)
        except Exception as e:
            logger.error(f"Join tournament DM error: {e}")
            await query.answer(
                "An error occurred while sending you a DM.", show_alert=True
            )

    # ... The rest of the function for refreshing the view remains the same

    if query and query.message:

        class MockChat:
            def __init__(self, chat_id):
                self.id = chat_id

        class MockMessage:
            def __init__(self, message_id, chat_id, reply_markup_orig):
                self.message_id = message_id
                self.chat = MockChat(chat_id)
                self.reply_markup = reply_markup_orig

            async def edit_text(self, text, parse_mode, reply_markup):
                await context.bot.edit_message_text(
                    chat_id=self.chat.id,
                    message_id=self.message_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                )

        if "view_tournaments" in str(query.message.text).lower() or (
            query.message.reply_markup
            and any(
                "view_tournaments" in button.callback_data
                for row in query.message.reply_markup.inline_keyboard
                for button in row
            )
        ):
            mock_cb_query = type(
                "MockCBQuery",
                (),
                {
                    "message": MockMessage(
                        query.message.message_id,
                        query.message.chat.id,
                        query.message.reply_markup,
                    ),
                    "answer": query.answer,
                    "data": "view_tournaments",
                },
            )
            mock_update_for_refresh = type(
                "MockUpdate",
                (),
                {
                    "callback_query": mock_cb_query,
                    "effective_message": mock_cb_query.message,
                    "effective_user": user,
                    "effective_chat": query.message.chat,
                },
            )
            try:
                await view_tournaments_handler(mock_update_for_refresh, context)
            except Exception as e_refresh:
                logger.error(
                    f"Failed to refresh tournament list after join: {e_refresh}"
                )


async def help_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the help guide. Now admin-only in groups."""

    # Get essential objects
    chat = update.effective_chat
    user = update.effective_user

    # Admin-Only Check (only for the typed command)
    if update.message and chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            if user.id not in [admin.user.id for admin in admins]:
                logger.info(f"Ignoring /help from non-admin {user.id} in group {chat.id}")
                return
        except Exception as e:
            logger.error(f"Failed to check admin status for /help: {e}")

    plain_text_parts = [
        "Efootball Super Bot Help Menu",
        "Here are the available commands, grouped by how you'll use them.",
        "",
        "--- General Commands ---",
        "/start - Shows the main menu to create or view tournaments.",
        "/help - Displays this help guide.",
        "/view_tournaments - Views tournaments. (Behavior changes if used in a group vs. a private chat).",
        "/leaderboard - Shows the global player rankings.",
        "",
        "--- Player Commands ---",
        "/mystats - Shows your personal tournament and match statistics.",
        "/h2h - Reply to a user's message to see your H2H stats against them.",
        "/report_score <MatchID> <You> <Opponent>",
        "  - Report the score for your match. Both players must report for it to be confirmed.",
        "/matchhistory - View a paginated history of your past matches.",
        "/view_matches <TournamentID>",
        "  - See the standings, groups, or brackets for a specific tournament.",
        "",
        "--- Creator & Admin Commands ---",
        "/create - Start the step-by-step wizard to create a new tournament.",
        "/start_tournament <TournamentID>",
        "  - Starts a pending tournament and generates all its matches.",
        "/set_announcement_group <TournamentID>",
        "  - Use this command inside a group to link it to your tournament.",
        "/broadcast <ID> <msg>",
        "  - Sends a message to all tournament participants.",
        "/remind_players <TournamentID>",
        "  - Manually sends a reminder to players in pending matches.",
        "/advance_swiss_round <TournamentID>",
        "  - (Swiss Only) Closes the current round and generates the next one.",
        "/conflict_resolve <MatchID> <P1Score> <P2Score>",
        "  - Manually sets the result for a disputed match.",
        "/add_player <ID> - Reply to a user to add them to a pending tournament.",
        '/award_badge <ID> "<Text>" - Reply to a user to give them a custom award.',
        "/cancel - Cancels the current process (like tournament creation).",
        "",
        "NOTE: Replace <...> with the actual ID numbers.",
    ]

    plain_text_message = "\n".join(plain_text_parts)
    text_to_send = escape_markdown(plain_text_message, version=2)

    query = update.callback_query
    if query:
        await query.answer()
        try:
            await query.edit_message_text(
                text=text_to_send, parse_mode="MarkdownV2", reply_markup=None
            )
        except Exception as e:
            logger.info(f"Help message was already up-to-date: {e}")
            pass
    else:
        await update.message.reply_text(text=text_to_send, parse_mode="MarkdownV2")


async def create_tournament_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the tournament creation conversation. Admin-only in groups."""
    
    # Get essential context objects
    chat = update.effective_chat
    user = update.effective_user

    # Admin-Only Check for Groups
    if update.message and chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            if user.id not in [admin.user.id for admin in admins]:
                logger.info(f"Ignoring /create from non-admin {user.id} in group {chat.id}")
                try:
                    await user.send_message("Hi! It's best to create tournaments here in our private chat to keep the group tidy. Just type /create to begin.")
                    if update.message:
                        await update.message.reply_text(f"I've sent you a private message to get started, {user.mention_html()}.", parse_mode='HTML')
                except Forbidden:
                    if update.message:
                        await update.message.reply_text(f"{user.mention_html()}, please start a private chat with me first to use the /create command.", parse_mode='HTML')
                return ConversationHandler.END
        except Exception as e:
            logger.error(f"Failed to check admin status for /create in group {chat.id}: {e}")

    # --- THIS IS THE CRITICAL PART THAT FIXES THE ERROR ---
    # It creates the 'folder' in the bot's memory before asking any questions.
    msg = escape_markdown_v2("Okay, let's create a new tournament! What should be its name?\n(You can /cancel at any time)")
    context.user_data['tournament_details'] = {}
    context.user_data['in_conversation'] = True
    # --- END OF CRITICAL PART ---
    
    query = update.callback_query
    if query:
        await query.answer()
        await query.edit_message_text(text=msg, parse_mode='MarkdownV2')
    elif update.message:
        await update.message.reply_text(text=msg, parse_mode='MarkdownV2')
            
    return ASK_TOURNAMENT_NAME


async def get_tournament_name(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Receives and stores the tournament name."""
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text(
            escape_markdown_v2("Name cannot be empty. Please provide a name."),
            parse_mode="MarkdownV2",
        )
        return ASK_TOURNAMENT_NAME
    if len(name) > 100:
        await update.message.reply_text(
            escape_markdown_v2(
                "That name is too long (maximum 100 characters). Please try a shorter name."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_TOURNAMENT_NAME
    context.user_data["tournament_details"]["name"] = name
    await update.message.reply_text(
        escape_markdown_v2(
            f"Great! The tournament name is set to: '{
                escape_markdown_v2(name)}'.\n\nNow, what game will be played? (e.g., eFootball , FIFA)"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_GAME_NAME


async def get_game_name(update: Update,
                        context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores the game name."""
    game = update.message.text.strip()
    if not game:
        await update.message.reply_text(
            escape_markdown_v2(
                "Game name cannot be empty. Please provide a game name."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_GAME_NAME
    if len(game) > 50:
        await update.message.reply_text(
            escape_markdown_v2(
                "That game name is too long (maximum 50 characters). Please try a shorter one."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_GAME_NAME
    context.user_data["tournament_details"]["game"] = game
    await update.message.reply_text(
        escape_markdown_v2(
            f"Game set to: '{
                escape_markdown_v2(game)}'.\n\nHow many participants can join? (Enter a number, e.g., 8, 16)"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_PARTICIPANT_COUNT


async def get_participant_count(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Receives and stores the maximum participant count."""
    try:
        count = int(update.message.text)
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2(
                "That doesn't look like a valid number. Please enter a number for the maximum participants."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_PARTICIPANT_COUNT
    if count < 2:
        await update.message.reply_text(
            escape_markdown_v2(
                "A tournament needs at least 2 participants. Please enter a higher number."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_PARTICIPANT_COUNT
    if count > 128:
        await update.message.reply_text(
            escape_markdown_v2(
                "That's a lot of players! Let's keep it to a maximum of 128 for now. Please enter a smaller number."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_PARTICIPANT_COUNT
    context.user_data["tournament_details"]["participants"] = count

    keyboard = [
        [
            InlineKeyboardButton(
                "🏆 Single Elimination", callback_data="single_elimination"
            )
        ],
        [InlineKeyboardButton("🔄 Round Robin", callback_data="round_robin")],
        [InlineKeyboardButton("🌍 League & Knockout",
                              callback_data="group_knockout")],
        [InlineKeyboardButton("♟️ Swiss", callback_data="swiss")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        escape_markdown_v2(
            f"Max participants: {count}.\n\nWhat type of tournament will this be?"
        ),
        parse_mode="MarkdownV2",
        reply_markup=reply_markup,
    )
    return ASK_TOURNAMENT_TYPE


async def get_tournament_type(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Receives and stores the tournament type, branching based on selection."""
    query = update.callback_query
    await query.answer()
    type_callback_data = query.data

    if type_callback_data == "single_elimination":
        context.user_data["tournament_details"]["type"] = "Single Elimination"
        message_text = "Tournament type: Single Elimination."
        await query.edit_message_text(
            escape_markdown_v2(
                f"{message_text}\n\nWhat's the match time? (e.g., '10 mins', '7 mins', 'Any minute you wish')"
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_TOURNAMENT_TIME
    elif type_callback_data == "round_robin":
        context.user_data["tournament_details"]["type"] = "Round Robin"
        message_text = "Tournament type: Round Robin."
        await query.edit_message_text(
            escape_markdown_v2(
                f"{message_text}\n\nWhat's the match time? (e.g., '10 mins/half', 'Best of 3', 'FT3')"
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_TOURNAMENT_TIME
    elif type_callback_data == "group_knockout":
        context.user_data["tournament_details"]["type"] = "Group Stage & Knockout"
        message_text = "Tournament type: Group Stage & Knockout."
        await query.edit_message_text(
            escape_markdown_v2(
                f"{message_text}\n\nHow many groups will this tournament have? (e.g., 4, 8)"
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_NUM_GROUPS
    elif type_callback_data == "swiss":
        context.user_data["tournament_details"]["type"] = "Swiss"
        message_text = "Tournament type: Swiss."
        await query.edit_message_text(
            escape_markdown_v2(
                f"{message_text}\n\nHow many rounds will this Swiss tournament have? (e.g., 5, 7)"
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_ROUNDS
    else:
        await query.edit_message_text(
            escape_markdown_v2("Invalid selection. Please try again."),
            parse_mode="MarkdownV2",
        )
        keyboard = [
            [
                InlineKeyboardButton(
                    "🏆 Single Elimination", callback_data="single_elimination"
                )
            ],
            [InlineKeyboardButton(
                "🔄 Round Robin", callback_data="round_robin")],
            [
                InlineKeyboardButton(
                    "🌍 League & Knockout", callback_data="group_knockout"
                )
            ],
            [InlineKeyboardButton("♟️ Swiss", callback_data="swiss")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            escape_markdown_v2("Please choose an available tournament type:"),
            "MarkdownV2",
            reply_markup=reply_markup,
        )
        return ASK_TOURNAMENT_TYPE


async def get_num_groups(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores the number of groups for Group Stage & Knockout tournaments."""
    try:
        num_groups = int(update.message.text)
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2(
                "That doesn't look like a valid number. Please enter a number for the number of groups."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_NUM_GROUPS

    total_participants = context.user_data["tournament_details"].get(
        "participants", 0)
    if num_groups < 1:
        await update.message.reply_text(
            escape_markdown_v2("You need at least 1 group."), parse_mode="MarkdownV2"
        )
        return ASK_NUM_GROUPS

    if num_groups > total_participants / 2 and total_participants >= 2:
        await update.message.reply_text(
            escape_markdown_v2(
                f"Too many groups for {total_participants} participants. Each group needs at least 2 players to have matches. Please enter a smaller number of groups."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_NUM_GROUPS

    if total_participants % num_groups != 0 and num_groups > 1:
        await update.message.reply_text(
            escape_markdown_v2(
                f"With {total_participants} participants, it's best to choose a number of groups that divides evenly, or allows for slightly uneven groups (e.g., 4 groups for 10 players means 2 groups of 3 and 2 of 2). You chose {num_groups}. Is this okay? Or enter a different number of groups. "
                f"If you're okay with uneven groups, just re-enter {num_groups}."
            ),
            parse_mode="MarkdownV2",
        )

    context.user_data["tournament_details"]["num_groups"] = num_groups
    await update.message.reply_text(
        escape_markdown_v2(
            f"Number of groups set to: {num_groups}.\n\nWhat's the match time? (e.g., '10 mins/half', 'Best of 3', 'FT3')"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_TOURNAMENT_TIME


async def get_swiss_rounds(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores the number of rounds for Swiss tournaments."""
    try:
        num_swiss_rounds = int(update.message.text)
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2(
                "That doesn't look like a valid number. Please enter a number for the number of Swiss rounds."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_ROUNDS

    if num_swiss_rounds < 1:
        await update.message.reply_text(
            escape_markdown_v2("A Swiss tournament needs at least 1 round."),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_ROUNDS

    max_players = context.user_data["tournament_details"].get(
        "participants", 0)
    recommended_min_rounds = math.ceil(
        math.log2(max_players)) if max_players > 1 else 1

    if num_swiss_rounds < recommended_min_rounds:
        await update.message.reply_text(
            escape_markdown_v2(
                f"For {max_players} participants, it's recommended to have at least {recommended_min_rounds} rounds for a meaningful Swiss tournament. Please enter a higher number, or re-enter {num_swiss_rounds} if you understand the implications."
            ),
            parse_mode="MarkdownV2",
        )

    context.user_data["tournament_details"]["num_swiss_rounds"] = num_swiss_rounds

    # After Swiss rounds, ask for knockout qualifiers
    await update.message.reply_text(
        escape_markdown_v2(
            f"Swiss rounds set to: {num_swiss_rounds}.\n\nHow many players will qualify for the knockout stage after the Swiss rounds? (Enter a number, e.g., 8, 16. Must be a power of 2 for a clean bracket, or BYEs will be used)"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_SWISS_KNOCKOUT_QUALIFIERS


async def get_swiss_knockout_qualifiers(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Receives and stores the number of players qualifying for Swiss knockout."""
    try:
        num_qualifiers = int(update.message.text)
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2(
                "That doesn't look like a valid number. Please enter a number for the knockout qualifiers."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_KNOCKOUT_QUALIFIERS

    total_participants = context.user_data["tournament_details"].get(
        "participants", 0)
    if num_qualifiers < 0:
        await update.message.reply_text(
            escape_markdown_v2(
                "Number of qualifiers cannot be negative. Enter 0 if no knockout stage."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_KNOCKOUT_QUALIFIERS
    if num_qualifiers > total_participants:
        await update.message.reply_text(
            escape_markdown_v2(
                f"Number of qualifiers ({num_qualifiers}) cannot exceed total participants ({total_participants}). Please enter a smaller number."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_KNOCKOUT_QUALIFIERS
    if num_qualifiers > 0 and num_qualifiers < 2:
        await update.message.reply_text(
            escape_markdown_v2(
                "If you have a knockout stage, you need at least 2 qualifiers. Enter 0 if no knockout stage."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_SWISS_KNOCKOUT_QUALIFIERS

    if num_qualifiers > 0 and not (
        num_qualifiers & (num_qualifiers - 1) == 0
    ):  # Check if power of 2
        await update.message.reply_text(
            escape_markdown_v2(
                f"It's highly recommended to choose a number of qualifiers that is a power of 2 (e.g., 2, 4, 8, 16) for a clean knockout bracket. You entered {num_qualifiers}. Is this okay? Or enter a different number. (BYEs will be used if not a power of 2)"
            ),
            parse_mode="MarkdownV2",
        )
        # Soft warning, user can re-enter same number to proceed.

    context.user_data["tournament_details"][
        "swiss_knockout_qualifiers"
    ] = num_qualifiers
    await update.message.reply_text(
        escape_markdown_v2(
            f"Knockout qualifiers set to: {
                num_qualifiers if num_qualifiers > 0 else 'None'}.\n\nWhat's the match time? (e.g., '10 mins/half', 'Best of 3', 'FT3')"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_TOURNAMENT_TIME


async def get_tournament_time(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Receives and stores the match time setting."""
    time_setting = update.message.text.strip()
    if not time_setting:
        await update.message.reply_text(
            escape_markdown_v2("Match time cannot be empty."), parse_mode="MarkdownV2"
        )
        return ASK_TOURNAMENT_TIME
    if len(time_setting) > 100:
        await update.message.reply_text(
            escape_markdown_v2(
                "That's a bit long for match time (max 100 chars). Try again."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_TOURNAMENT_TIME
    context.user_data["tournament_details"]["tournament_time"] = time_setting

    keyboard = [
        [
            InlineKeyboardButton("PK: ON", callback_data="pk_on"),
            InlineKeyboardButton("PK: OFF", callback_data="pk_off"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        escape_markdown_v2(
            f"Match time: '{
                escape_markdown_v2(time_setting)}'.\n\nWill penalties (PK) be ON or OFF? (Relevant for games like FIFA/eFootball)"
        ),
        parse_mode="MarkdownV2",
        reply_markup=reply_markup,
    )
    return ASK_PENALTIES


async def get_penalties(update: Update,
                        context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores the penalties setting."""
    query = update.callback_query
    await query.answer()
    context.user_data["tournament_details"]["penalties"] = (
        "ON" if query.data == "pk_on" else "OFF"
    )

    keyboard = [
        [
            InlineKeyboardButton("ET: ON", callback_data="et_on"),
            InlineKeyboardButton("ET: OFF", callback_data="et_off"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        escape_markdown_v2(
            f"Penalties: {
                context.user_data['tournament_details']['penalties']}.\n\nWill extra time (ET) be ON or OFF?"
        ),
        parse_mode="MarkdownV2",
        reply_markup=reply_markup,
    )
    return ASK_EXTRA_TIME


async def get_extra_time(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores the extra time setting."""
    query = update.callback_query
    await query.answer()
    context.user_data["tournament_details"]["extra_time"] = (
        "ON" if query.data == "et_on" else "OFF"
    )
    await query.edit_message_text(
        escape_markdown_v2(
            f"Extra Time: {
                context.user_data['tournament_details']['extra_time']}.\n\nAny other specific conditions or rules? (e.g., Good, Normal, Bad, Excellent, 'Classic squads only', 'No custom tactics'. Max 500 chars)"
        ),
        parse_mode="MarkdownV2",
    )
    return ASK_CONDITIONS


async def get_conditions(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receives and stores any additional tournament conditions."""
    conditions = update.message.text.strip()
    if len(conditions) > 500:
        await update.message.reply_text(
            escape_markdown_v2(
                "Those conditions are too long (max 500 chars). Please shorten them or type 'None'."
            ),
            parse_mode="MarkdownV2",
        )
        return ASK_CONDITIONS
    context.user_data["tournament_details"]["conditions"] = (
        conditions if conditions.lower() != "none" else None
    )

    td = context.user_data["tournament_details"]
    summary_lines = [
        escape_markdown_v2("📝 *Review Tournament Details:*"),
        escape_markdown_v2(f"  Name: *{td.get('name', 'N/A')}*"),
        escape_markdown_v2(f"  Game: *{td.get('game', 'N/A')}*"),
        escape_markdown_v2(
            f"  Max Players: *{str(td.get('participants', 'N/A'))}*"),
        escape_markdown_v2(f"  Type: *{td.get('type', 'N/A')}*"),
    ]
    if td.get("type") == "Group Stage & Knockout":
        summary_lines.append(
            escape_markdown_v2(
                f"  Number of Groups: *{str(td.get('num_groups', 'N/A'))}*"
            )
        )
    elif td.get("type") == "Swiss":
        summary_lines.append(
            escape_markdown_v2(
                f"  Number of Rounds: *{str(td.get('num_swiss_rounds', 'N/A'))}*"
            )
        )
        if td.get("swiss_knockout_qualifiers"):
            summary_lines.append(
                escape_markdown_v2(
                    f"  Knockout Qualifiers: *{str(td.get('swiss_knockout_qualifiers', 'N/A'))}*"
                )
            )
        else:
            summary_lines.append(
                escape_markdown_v2("  Knockout Qualifiers: *None*"))

    summary_lines.extend(
        [
            escape_markdown_v2(
                f"  Time: *{td.get('tournament_time', 'N/A')}*"),
            escape_markdown_v2(f"  Penalties: *{td.get('penalties', 'N/A')}*"),
            escape_markdown_v2(
                f"  Extra Time: *{td.get('extra_time', 'N/A')}*"),
            escape_markdown_v2(
                f"  Conditions: *{
                    td.get('conditions') if td.get('conditions') else 'None'}*"
            ),
            escape_markdown_v2("\nLooking good? 👍"),
        ]
    )
    summary_escaped = "\n".join(summary_lines)

    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Save Tournament", callback_data="confirm_save_tournament"
            )
        ],
        [
            InlineKeyboardButton(
                "✏️ Edit (Start Over)", callback_data="edit_tournament_details"
            )
        ],
        [
            InlineKeyboardButton(
                "❌ Cancel Creation", callback_data="cancel_final_confirmation"
            )
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        summary_escaped, parse_mode="MarkdownV2", reply_markup=reply_markup
    )
    return CONFIRM_SAVE_TOURNAMENT


async def handle_final_confirmation(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Handles the final confirmation for tournament creation."""
    query = update.callback_query
    await query.answer()
    choice = query.data

    if choice == "confirm_save_tournament":
        tournament_details_to_save = context.user_data.get(
            "tournament_details", {})
        if not tournament_details_to_save.get("name"):
            await query.edit_message_text(
                escape_markdown_v2(
                    "⚠️ An error occurred. Details were lost. Please /start again."
                ),
                parse_mode="MarkdownV2",
            )
            context.user_data.clear()
            context.user_data["in_conversation"] = False
            return ConversationHandler.END

        db_details = tournament_details_to_save.copy()
        db_details["id"] = str(uuid.uuid4())[:8]
        db_details["creator_id"] = update.effective_user.id
        db_details["status"] = "pending"
        db_details["group_chat_id"] = None
        db_details["num_groups"] = tournament_details_to_save.get(
            "num_groups", None)
        db_details["num_swiss_rounds"] = tournament_details_to_save.get(
            "num_swiss_rounds", None
        )
        db_details["current_swiss_round"] = 0
        db_details["swiss_knockout_qualifiers"] = tournament_details_to_save.get(
            "swiss_knockout_qualifiers", None
        )

        if add_tournament_to_db(db_details):
            t_name_esc = escape_markdown_v2(db_details["name"])
            t_type_esc = escape_markdown_v2(
                db_details.get("type", "Unknown Type"))
            t_id_raw = db_details["id"]
            message_text = (
                f"🎉 {t_type_esc} Tournament '{t_name_esc}' has been created successfully\\!\n"
                f"Tournament ID: `{t_id_raw}`\n\n"
                f"ℹ️ Players can now join if this tournament is listed via `/view_tournaments`\\.\n"
                f"To start it, use: `/start_tournament {t_id_raw}`\n"
                f"To set an announcement group for this tournament, go to that group and use: `/set_announcement_group {t_id_raw}`"
            )
            await query.edit_message_text(message_text, parse_mode="MarkdownV2")
        else:
            await query.edit_message_text(
                escape_markdown_v2(
                    "⚠️ There was an error saving the tournament to the database. Please try again later."
                ),
                parse_mode="MarkdownV2",
            )
    elif choice == "edit_tournament_details":
        await query.edit_message_text(
            escape_markdown_v2(
                "Okay, let's start over. Please use /create to begin a new tournament creation process."
            ),
            parse_mode="MarkdownV2",
        )
    elif choice == "cancel_final_confirmation":
        await query.edit_message_text(
            escape_markdown_v2("Tournament creation cancelled."),
            parse_mode="MarkdownV2",
        )

    context.user_data.clear()
    context.user_data["in_conversation"] = False
    return ConversationHandler.END


async def cancel_conversation(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Cancels any ongoing conversation."""
    message_text = escape_markdown_v2(
        "The current operation has been cancelled. Use /start to see available commands."
    )
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(
                text=message_text, parse_mode="MarkdownV2", reply_markup=None
            )
        except Exception:
            if update.effective_chat:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message_text,
                    parse_mode="MarkdownV2",
                )
    elif update.message:
        await update.message.reply_text(text=message_text, parse_mode="MarkdownV2")

    context.user_data.clear()
    context.user_data["in_conversation"] = False
    return ConversationHandler.END


async def error_handler(update: object,
                        context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors and notifies the user."""
    logger.error(
        f"Update {update} caused error {context.error}", exc_info=context.error
    )

    if isinstance(context.error, Forbidden):
        logger.warning(
            f"Forbidden error: {
                context.error}. Bot might be blocked or kicked from a group."
        )
        return

    if update and hasattr(
            update, "effective_message") and update.effective_message:
        try:
            await update.effective_message.reply_text(
                escape_markdown_v2(
                    "⚠️ Oops! Something went wrong. The developers have been notified."
                ),
                parse_mode="MarkdownV2",
            )
        except Exception as e:
            logger.error(f"Failed to send error handler message to user: {e}")
    elif (
        update
        and hasattr(update, "callback_query")
        and update.callback_query
        and update.callback_query.message
    ):
        try:
            await context.bot.send_message(
                chat_id=update.callback_query.message.chat_id,
                text=escape_markdown_v2(
                    "⚠️ Oops! Something went wrong with that action. The developers have been notified."
                ),
                parse_mode="MarkdownV2",
            )
        except Exception as e:
            logger.error(
                f"Failed to send error handler message to user via callback_query: {e}"
            )


async def start_tournament_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts a tournament, generating matches based on its type."""

    # Step 1: Define these variables first so the check can use them.
    chat = update.effective_chat
    user = update.effective_user
    
    # Step 2: Place the Admin-Only check right here.
    if chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            admin_ids = [admin.user.id for admin in admins]
            if user.id not in admin_ids:
                logger.info(f"Ignoring /start_tournament from non-admin {user.id} in group {chat.id}")
                return # Stop the function for non-admins
        except Exception as e:
            logger.error(f"Failed to check admin status for /start_tournament in group {chat.id}: {e}")

    user_id = update.effective_user.id
    args = context.args
    if not args:
        await update.message.reply_text(escape_markdown_v2("Please provide the Tournament ID. Usage: /start_tournament <Tournament_ID>"), parse_mode='MarkdownV2')
        return

    t_id = args[0]
    tournament = get_tournament_details_by_id(t_id)
   
    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Tournament with ID `{
                    escape_markdown_v2(t_id)}` not found."
            ),
            parse_mode="MarkdownV2",
        )
        return

    # This check is still useful as the primary authorization for the
    # command's action
    if tournament["creator_id"] != user_id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Only the creator of the tournament can start it."),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["status"] != "pending":
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ This tournament is not pending. Current status: {
                    escape_markdown_v2(
                        tournament['status'])}."
            ),
            parse_mode="MarkdownV2",
        )
        return

    registered_players = get_registered_players(t_id)
    num_registered = len(registered_players)

    # Handle single player auto-win (applies to any type if only one player)
    if num_registered == 1:
        winner = registered_players[0]
        winner_id = winner["user_id"]
        winner_display_name = winner["username"]
        if update_tournament_status(
                t_id, "completed", winner_id, winner_display_name):
            reply_msg = (
                f"🎉 Tournament *{
                    escape_markdown_v2(
                        tournament['name'])}* started & auto\\-completed\\!\n"
                f"Only one player, {
                    escape_markdown_v2(winner_display_name)}, is the winner by default\\!"
            )
            await update.message.reply_text(reply_msg, parse_mode="MarkdownV2")
            public_auto_complete_msg = (
                f"🎉 Tournament *{
                    escape_markdown_v2(
                        tournament['name'])}* auto\\-completed due to a single participant\\!\n"
                f"🏆 Winner: *{escape_markdown_v2(winner_display_name)}*"
            )
            await send_public_announcement(context, t_id, public_auto_complete_msg)
            update_leaderboard(winner_id, winner_display_name)
            updated_t_details = get_tournament_details_by_id(t_id)
            if updated_t_details:
                await send_tournament_glory_board(
                    context, updated_t_details, winner_id, winner_display_name
                )
        else:
            await update.message.reply_text(
                escape_markdown_v2(
                    "⚠️ Failed to auto-complete tournament for a single player."
                ),
                parse_mode="MarkdownV2",
            )
        return

    if num_registered < 2:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ At least 2 players are needed to start. Currently {num_registered} registered."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if not update_tournament_status(t_id, "ongoing"):
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Failed to update tournament status to 'ongoing'. Please try again."
            ),
            parse_mode="MarkdownV2",
        )
        return

    parts = [
        f"🎉 Tournament *{
            escape_markdown_v2(
                tournament['name'])}* \\({
            escape_markdown_v2(
                tournament['type'])}\\) has been started\\! Status: `ongoing`"
    ]
    # Shuffle players for initial seeding/grouping
    random.shuffle(registered_players)

    if tournament["type"] == "Single Elimination":
        public_start_message = f"🎉 Tournament *{
            escape_markdown_v2(
                tournament['name'])}* \\(Single Elimination\\) has officially started\\!"
        public_start_message += (
            "\nMatches have been generated\\. Good luck to all participants\\!"
        )
        await send_public_announcement(context, t_id, public_start_message)

        parts.append(
            escape_markdown_v2(
                "\n🔥 *Single Elimination Bracket Generation Initiated...*"
            )
        )
        num_players = len(registered_players)
        num_rounds_full_bracket = (
            math.ceil(math.log2(num_players)) if num_players > 0 else 0
        )
        full_bracket_size = 2**num_rounds_full_bracket if num_players > 0 else 0

        current_round_participants_data = list(registered_players) + [
            {"user_id": None, "username": "BYE"}
        ] * (full_bracket_size - num_players)
        random.shuffle(
            current_round_participants_data
        )  # Shuffle again for bracket fairness

        active_nodes_for_next_round = []
        temp_player_processing_list = list(current_round_participants_data)

        match_in_round_idx_r1 = 0
        while temp_player_processing_list:
            p1_data = temp_player_processing_list.pop(0)
            if (
                not temp_player_processing_list
            ):  # Odd number of players left, this one gets a bye
                if p1_data["user_id"] is not None:
                    active_nodes_for_next_round.append(
                        {
                            "type": "player",
                            "user_id": p1_data["user_id"],
                            "username": p1_data["username"],
                        }
                    )
                    parts.append(
                        f"  R1: {
                            escape_markdown_v2(
                                p1_data['username'])} gets a BYE into the next stage of bracket building\\."
                    )
                break

            p2_data = temp_player_processing_list.pop(0)
            match_in_round_idx_r1 += 1

            if p1_data["user_id"] is not None and p2_data["user_id"] is not None:
                m_dets_r1 = {
                    "tournament_id": t_id,
                    "round_number": 1,
                    "match_in_round_index": match_in_round_idx_r1,
                    "player1_user_id": p1_data["user_id"],
                    "player1_username": p1_data["username"],
                    "player2_user_id": p2_data["user_id"],
                    "player2_username": p2_data["username"],
                    "status": "scheduled",
                    "next_match_id": None,
                }
                m_id_r1 = add_match_to_db(m_dets_r1)
                if m_id_r1:
                    active_nodes_for_next_round.append(
                        {"type": "match", "id": m_id_r1})
                    parts.append(
                        f"  R1 M{match_in_round_idx_r1}: {
                            escape_markdown_v2(
                                m_dets_r1['player1_username'])} vs {
                            escape_markdown_v2(
                                m_dets_r1['player2_username'])} \\(ID: `{m_id_r1}`\\)"
                    )
                    await notify_players_of_match(
                        context,
                        m_id_r1,
                        t_id,
                        tournament["name"],
                        p1_data["user_id"],
                        p1_data["username"],
                        p2_data["user_id"],
                        p2_data["username"],
                    )
            elif p1_data["user_id"] is not None:  # p2 is BYE
                active_nodes_for_next_round.append(
                    {
                        "type": "player",
                        "user_id": p1_data["user_id"],
                        "username": p1_data["username"],
                    }
                )
                parts.append(
                    f"  R1: {
                        escape_markdown_v2(
                            p1_data['username'])} gets a BYE \\(vs virtual BYE player\\)\\."
                )
            elif p2_data["user_id"] is not None:  # p1 is BYE
                active_nodes_for_next_round.append(
                    {
                        "type": "player",
                        "user_id": p2_data["user_id"],
                        "username": p2_data["username"],
                    }
                )
                parts.append(
                    f"  R1: {
                        escape_markdown_v2(
                            p2_data['username'])} gets a BYE \\(vs virtual BYE player\\)\\."
                )

        current_round_num_shells = 1
        while len(active_nodes_for_next_round) > 1:
            match_in_idx_shell = 0
            temp_active_shell_nodes = list(active_nodes_for_next_round)
            active_nodes_for_next_round.clear()
            current_round_num_shells += 1

            while temp_active_shell_nodes:
                node1_adv = temp_active_shell_nodes.pop(0)
                if not temp_active_shell_nodes:
                    active_nodes_for_next_round.append(node1_adv)
                    adv_name = node1_adv.get(
                        "username", f"Match Winner of {node1_adv.get('id')}"
                    )
                    logger.info(
                        f"Node {adv_name} gets bye to next shell round {
                            current_round_num_shells + 1}."
                    )
                    break

                node2_adv = temp_active_shell_nodes.pop(0)
                match_in_idx_shell += 1
                shell_dets = {
                    "tournament_id": t_id,
                    "round_number": current_round_num_shells,
                    "match_in_round_index": match_in_idx_shell,
                    "player1_user_id": None,
                    "player1_username": None,
                    "player2_user_id": None,
                    "player2_username": None,
                    "status": "pending_players",
                    "next_match_id": None,
                }

                if node1_adv["type"] == "player":
                    shell_dets["player1_user_id"] = node1_adv["user_id"]
                    shell_dets["player1_username"] = node1_adv["username"]
                if node2_adv["type"] == "player":
                    if shell_dets["player1_user_id"] is None:
                        shell_dets["player1_user_id"] = node2_adv["user_id"]
                        shell_dets["player1_username"] = node2_adv["username"]
                    else:
                        shell_dets["player2_user_id"] = node2_adv["user_id"]
                        shell_dets["player2_username"] = node2_adv["username"]

                if shell_dets["player1_user_id"] and shell_dets["player2_user_id"]:
                    shell_dets["status"] = "scheduled"
                    parts.append(
                        f"  R{current_round_num_shells} M{match_in_idx_shell} \\(Auto\\-Scheduled BYE vs BYE\\): {
                            escape_markdown_v2(
                                shell_dets['player1_username'])} vs {
                            escape_markdown_v2(
                                shell_dets['player2_username'])}"
                    )

                new_shell_id = add_match_to_db(shell_dets)
                if not new_shell_id:
                    logger.error(
                        f"CRITICAL: Failed to create shell match R{current_round_num_shells}M{match_in_idx_shell}. Tournament {t_id} may be inconsistent."
                    )
                    continue
                active_nodes_for_next_round.append(
                    {"type": "match", "id": new_shell_id}
                )

                conn_link = sqlite3.connect(DB_NAME)
                cur_link = conn_link.cursor()
                try:
                    if node1_adv["type"] == "match":
                        cur_link.execute(
                            "UPDATE matches SET next_match_id = ? WHERE match_id = ?",
                            (new_shell_id, node1_adv["id"]),
                        )
                    if node2_adv["type"] == "match":
                        cur_link.execute(
                            "UPDATE matches SET next_match_id = ? WHERE match_id = ?",
                            (new_shell_id, node2_adv["id"]),
                        )
                    conn_link.commit()
                except sqlite3.Error as e_link:
                    logger.error(
                        f"Error linking previous matches to shell {new_shell_id}: {e_link}"
                    )
                finally:
                    conn_link.close()

                if shell_dets["status"] == "scheduled":
                    await notify_players_of_match(
                        context,
                        new_shell_id,
                        t_id,
                        tournament["name"],
                        shell_dets["player1_user_id"],
                        shell_dets["player1_username"],
                        shell_dets["player2_user_id"],
                        shell_dets["player2_username"],
                    )
            current_round_num_shells += 1

        if (
            len(active_nodes_for_next_round) == 1
            and active_nodes_for_next_round[0]["type"] == "match"
        ):
            parts.append(
                escape_markdown_v2(
                    f"\nBracket created successfully! Final Match ID will be: `{
                        active_nodes_for_next_round[0]['id']}`."
                )
            )
        elif not active_nodes_for_next_round and num_players > 0:
            parts.append(
                escape_markdown_v2(
                    "\n⚠️ Bracket generation completed, but no final match node identified. Check logs."
                )
            )
        elif len(active_nodes_for_next_round) > 1:
            parts.append(
                escape_markdown_v2(
                    "\n⚠️ Bracket generation completed with multiple final nodes. This indicates an issue."
                )
            )

        if num_players > 1:
            parts.append(
                escape_markdown_v2(
                    "\nGood luck to all participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
                )
            )

    elif tournament["type"] == "Round Robin":
        parts.append(escape_markdown_v2(
            "\n🗓️ *Round Robin Fixture Generation...*"))
        conn_rr = sqlite3.connect(DB_NAME)
        cursor_rr = conn_rr.cursor()
        try:
            # Initialize standings for all registered players
            for player in registered_players:
                cursor_rr.execute(
                    """
                    INSERT INTO round_robin_standings (tournament_id, user_id, username, games_played, wins, draws, losses, goals_for, goals_against, goal_difference, points)
                    VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
                    ON CONFLICT(tournament_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                    (t_id, player["user_id"], player["username"]),
                )
            conn_rr.commit()
            logger.info(f"Initialized Round Robin standings for T_ID {t_id}")

            # Generate fixtures
            rr_schedule = generate_round_robin_fixtures(registered_players)
            if not rr_schedule:
                parts.append(
                    escape_markdown_v2(
                        "⚠️ Could not generate a valid Round Robin schedule. Ensure enough players are registered."
                    )
                )
                await update.message.reply_text(
                    "\n".join(parts), parse_mode="MarkdownV2"
                )
                return

            total_matches_generated = 0
            for round_num, round_matches in enumerate(rr_schedule):
                current_round_number = round_num + 1
                parts.append(
                    f"\n*{
                        escape_markdown_v2(
                            f'--- Match Day {current_round_number} ---')}*"
                )
                match_in_round_idx = 0
                for p1_data, p2_data in round_matches:
                    match_in_round_idx += 1
                    m_dets = {
                        "tournament_id": t_id,
                        "round_number": current_round_number,
                        "match_in_round_index": match_in_round_idx,
                        "player1_user_id": p1_data["user_id"],
                        "player1_username": p1_data["username"],
                        "player2_user_id": p2_data["user_id"],
                        "player2_username": p2_data["username"],
                        "status": "scheduled",
                        "next_match_id": None,
                    }
                    m_id = add_match_to_db(m_dets)
                    if m_id:
                        total_matches_generated += 1
                        parts.append(
                            f"  M\\-ID `{m_id}`: {
                                escape_markdown_v2(
                                    p1_data['username'])} vs {
                                escape_markdown_v2(
                                    p2_data['username'])}"
                        )
                        await notify_players_of_match(
                            context,
                            m_id,
                            t_id,
                            tournament["name"],
                            p1_data["user_id"],
                            p1_data["username"],
                            p2_data["user_id"],
                            p2_data["username"],
                        )
                    else:
                        logger.error(
                            f"Failed to add RR match to DB for T_ID {t_id}, round {current_round_number}."
                        )

            if total_matches_generated > 0:
                parts.append(
                    escape_markdown_v2(
                        f"\nRound Robin fixtures generated successfully ({total_matches_generated} matches in total)!"
                    )
                )
                parts.append(
                    escape_markdown_v2(
                        "Good luck to all participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
                    )
                )
                public_start_message_rr = (
                    f"🎉 Tournament *{
                        escape_markdown_v2(
                            tournament['name'])}* \\(Round Robin\\) has officially started\\!\n"
                    f"Matches for all rounds have been generated\\. Check your DMs for match notifications\\! "
                    f"View standings and matches with `/view_matches {
                        escape_markdown_v2(t_id)}`\\."
                )
                await send_public_announcement(context, t_id, public_start_message_rr)
            else:
                parts.append(
                    escape_markdown_v2(
                        "⚠️ No matches were generated for this Round Robin tournament. This might indicate an issue with player registration or fixture generation logic."
                    )
                )
        except sqlite3.Error as e_rr_init:
            logger.error(
                f"Error initializing Round Robin standings or generating fixtures for T_ID {t_id}: {e_rr_init}"
            )
            parts.append(
                escape_markdown_v2(
                    "⚠️ Error initializing standings or generating fixtures for Round Robin tournament."
                )
            )
        finally:
            conn_rr.close()

    elif tournament["type"] == "Group Stage & Knockout":
        parts.append(
            escape_markdown_v2(
                "\n🌍 *Group Stage & Knockout Tournament Setup...*")
        )
        num_groups = tournament.get("num_groups")
        if not num_groups or num_groups <= 0:
            await update.message.reply_text(
                escape_markdown_v2(
                    "⚠️ Number of groups not set for this tournament. Please contact an admin to correct it or recreate the tournament."
                ),
                parse_mode="MarkdownV2",
            )
            return

        players_per_group = num_registered // num_groups
        remaining_players = num_registered % num_groups

        # Distribute players into groups
        # Randomize players before distribution
        random.shuffle(registered_players)
        groups_data = (
            []
        )  # List of {'group_id': int, 'group_name': str, 'players': list}
        player_index = 0
        for i in range(num_groups):
            group_name = f"Group {chr(65 + i)}"
            group_id = add_group_to_db(t_id, group_name)
            if not group_id:
                await update.message.reply_text(
                    escape_markdown_v2(
                        f"⚠️ Error creating group {group_name}. Tournament cannot proceed."
                    ),
                    parse_mode="MarkdownV2",
                )
                return

            current_group_players = []
            group_size = players_per_group + \
                (1 if i < remaining_players else 0)
            for _ in range(group_size):
                if player_index < num_registered:
                    player = registered_players[player_index]
                    add_player_to_group_db(
                        group_id, player["user_id"], player["username"]
                    )
                    current_group_players.append(player)
                    player_index += 1
            groups_data.append(
                {
                    "group_id": group_id,
                    "group_name": group_name,
                    "players": current_group_players,
                }
            )
            parts.append(
                escape_markdown_v2(
                    f"  Group '{group_name}' created with {
                        len(current_group_players)} players."
                )
            )

        # Generate fixtures for each group (Round Robin within groups)
        total_group_matches = 0
        for group in groups_data:
            group_id = group["group_id"]
            group_name = group["group_name"]
            group_players = group["players"]

            if len(group_players) < 2:
                parts.append(
                    escape_markdown_v2(
                        f"  ⚠️ Not enough players in Group '{group_name}' to generate matches. Skipping group matches."
                    )
                )
                continue

            parts.append(
                f"\n*{
                    escape_markdown_v2(
                        f'--- Generating matches for {group_name} ---')}*"
            )
            group_schedule = generate_round_robin_fixtures(group_players)
            for round_num, round_matches in enumerate(group_schedule):
                current_round_number = round_num + 1
                parts.append(
                    f"\n*{
                        escape_markdown_v2(
                            f'-- {group_name} Match Day {current_round_number} --')}*"
                )
                match_in_round_idx = 0
                for p1_data, p2_data in round_matches:
                    match_in_round_idx += 1
                    m_dets = {
                        "tournament_id": t_id,
                        "round_number": current_round_number,
                        "match_in_round_index": match_in_round_idx,
                        "player1_user_id": p1_data["user_id"],
                        "player1_username": p1_data["username"],
                        "player2_user_id": p2_data["user_id"],
                        "player2_username": p2_data["username"],
                        "status": "scheduled",
                        "next_match_id": None,
                        "group_id": group_id,
                    }
                    m_id = add_match_to_db(m_dets)
                    if m_id:
                        total_group_matches += 1
                        parts.append(
                            f"  M\\-ID `{m_id}`: {
                                escape_markdown_v2(
                                    p1_data['username'])} vs {
                                escape_markdown_v2(
                                    p2_data['username'])}"
                        )
                        await notify_players_of_match(
                            context,
                            m_id,
                            t_id,
                            tournament["name"],
                            p1_data["user_id"],
                            p1_data["username"],
                            p2_data["user_id"],
                            p2_data["username"],
                        )
                    else:
                        logger.error(
                            f"Failed to add Group Stage match to DB for T_ID {t_id}, group {group_name}, round {current_round_number}."
                        )

        if total_group_matches > 0:
            parts.append(
                escape_markdown_v2(
                    f"\nGroup stage fixtures generated successfully ({total_group_matches} matches in total)!"
                )
            )
            parts.append(
                escape_markdown_v2(
                    "Good luck to all participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
                )
            )
            parts.append(
                escape_markdown_v2(
                    f"You can view group standings and matches with `/view_matches {
                        escape_markdown_v2(t_id)}`\\."
                )
            )
            public_start_message_gs = (
                f"🎉 Tournament *{
                    escape_markdown_v2(
                        tournament['name'])}* \\(Group Stage & Knockout\\) has officially started\\!\n"
                f"Group stage matches have been generated\\. Check your DMs for notifications\\! "
                f"View group standings and matches with `/view_matches {
                    escape_markdown_v2(t_id)}`\\."
            )
            await send_public_announcement(context, t_id, public_start_message_gs)
        else:
            parts.append(
                escape_markdown_v2(
                    "⚠️ No matches were generated for the group stage. This might indicate an issue with player registration or group setup."
                )
            )

    elif tournament["type"] == "Swiss":
        parts.append(escape_markdown_v2(
            "\n♟️ *Swiss Tournament Round 1 Generation...*"))
        num_swiss_rounds = tournament.get("num_swiss_rounds")
        if not num_swiss_rounds or num_swiss_rounds <= 0:
            await update.message.reply_text(
                escape_markdown_v2(
                    "⚠️ Number of Swiss rounds not set for this tournament. Please contact an admin to correct it or recreate the tournament."
                ),
                parse_mode="MarkdownV2",
            )
            return

        # Initialize standings for all registered players
        conn_swiss_init = sqlite3.connect(DB_NAME)
        cursor_swiss_init = conn_swiss_init.cursor()
        try:
            for player in registered_players:
                cursor_swiss_init.execute(
                    """
                    INSERT INTO round_robin_standings (tournament_id, user_id, username, games_played, wins, draws, losses, goals_for, goals_against, goal_difference, points)
                    VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
                    ON CONFLICT(tournament_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                    (t_id, player["user_id"], player["username"]),
                )
            conn_swiss_init.commit()
            logger.info(f"Initialized Swiss standings for T_ID {t_id}")
        except sqlite3.Error as e_swiss_init:
            logger.error(
                f"Error initializing Swiss standings for T_ID {t_id}: {e_swiss_init}"
            )
            parts.append(
                escape_markdown_v2(
                    "⚠️ Error initializing standings for Swiss tournament."
                )
            )
            await update.message.reply_text("\n".join(parts), parse_mode="MarkdownV2")
            return
        finally:
            conn_swiss_init.close()

        # Set current Swiss round to 1
        if not update_tournament_swiss_round(t_id, 1):
            await update.message.reply_text(
                escape_markdown_v2(
                    "⚠️ Failed to set current Swiss round. Please try again."
                ),
                parse_mode="MarkdownV2",
            )
            return
        # Update in memory for immediate use
        tournament["current_swiss_round"] = 1

        # Generate matches for Round 1
        swiss_round_1_matches = generate_swiss_round_matches(
            t_id, 1, registered_players
        )
        total_matches_generated = 0
        if not swiss_round_1_matches:
            parts.append(
                escape_markdown_v2(
                    "⚠️ Could not generate matches for Swiss Round 1. Ensure enough players are registered."
                )
            )
            await update.message.reply_text("\n".join(parts), parse_mode="MarkdownV2")
            return

        parts.append(f"\n*{escape_markdown_v2(f'--- Swiss Round 1 ---')}*")
        for m_dets in swiss_round_1_matches:
            m_id = add_match_to_db(m_dets)
            if m_id:
                total_matches_generated += 1
                if m_dets["status"] == "bye":
                    parts.append(
                        f"  M\\-ID `{m_id}`: {
                            escape_markdown_v2(
                                m_dets['player1_username'])} gets a *BYE*"
                    )
                else:
                    parts.append(
                        f"  M\\-ID `{m_id}`: {
                            escape_markdown_v2(
                                m_dets['player1_username'])} vs {
                            escape_markdown_v2(
                                m_dets['player2_username'])}"
                    )
                    await notify_players_of_match(
                        context,
                        m_id,
                        t_id,
                        tournament["name"],
                        m_dets["player1_user_id"],
                        m_dets["player1_username"],
                        m_dets["player2_user_id"],
                        m_dets["player2_username"],
                    )
            else:
                logger.error(
                    f"Failed to add Swiss match to DB for T_ID {t_id}, round 1."
                )

        if total_matches_generated > 0:
            parts.append(
                escape_markdown_v2(
                    f"\nSwiss Round 1 fixtures generated successfully ({total_matches_generated} matches in total)!"
                )
            )
            parts.append(
                escape_markdown_v2(
                    "Good luck to all participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
                )
            )
            parts.append(
                escape_markdown_v2(
                    f"You can view standings and matches with `/view_matches {
                        escape_markdown_v2(t_id)}`\\."
                )
            )
            public_start_message_swiss = (
                f"🎉 Tournament *{
                    escape_markdown_v2(
                        tournament['name'])}* \\(Swiss\\) has officially started\\!\n"
                f"Round 1 matches have been generated\\. Check your DMs for notifications\\! "
                f"View standings and matches with `/view_matches {
                    escape_markdown_v2(t_id)}`\\."
            )
            await send_public_announcement(context, t_id, public_start_message_swiss)
        else:
            parts.append(
                escape_markdown_v2(
                    "⚠️ No matches were generated for Swiss Round 1. This might indicate an issue with player registration or pairing logic."
                )
            )

    else:
        parts.append(
            escape_markdown_v2(
                f"\n⚠️ Match generation for tournament type '{
                    escape_markdown_v2(
                        tournament['type'])}' is not yet implemented."
            )
        )
        public_start_message_other = f"🎉 Tournament *{
            escape_markdown_v2(
                tournament['name'])}* has started\\!\nMatch generation for '{
            escape_markdown_v2(
                tournament['type'])}' is not yet implemented\\."
        await send_public_announcement(context, t_id, public_start_message_other)

    await update.message.reply_text("\n".join(parts), parse_mode="MarkdownV2")


async def advance_swiss_round_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Allows tournament creator to advance to the next Swiss round."""
    user_id = update.effective_user.id
    args = context.args
    is_callback = bool(update.callback_query and update.callback_query.message)
    reply_method = (
        update.effective_message.edit_text
        if is_callback
        else (update.message.reply_text if update.message else None)
    )

    if not reply_method:
        logger.error(
            "advance_swiss_round_command: No valid reply method found.")
        if update.callback_query:
            await update.callback_query.answer(
                "Error displaying data.", show_alert=True
            )
        return

    if not args:
        await reply_method(
            text=escape_markdown_v2(
                "Please provide the Tournament ID. Usage: /advance_swiss_round <Tournament_ID>"
            ),
            parse_mode="MarkdownV2",
        )
        return

    t_id = args[0]
    tournament = get_tournament_details_by_id(t_id)

    if not tournament:
        await reply_method(
            text=escape_markdown_v2(
                f"⚠️ Tournament with ID `{
                    escape_markdown_v2(t_id)}` not found."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["creator_id"] != user_id:
        await reply_method(
            text=escape_markdown_v2(
                "⚠️ Only the creator of the tournament can advance Swiss rounds."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["type"] != "Swiss":
        await reply_method(
            text=escape_markdown_v2(
                f"⚠️ Tournament `{
                    escape_markdown_v2(t_id)}` is not a Swiss tournament."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["status"] != "ongoing":
        await reply_method(
            text=escape_markdown_v2(
                f"⚠️ Tournament `{
                    escape_markdown_v2(t_id)}` is not ongoing. Current status: {
                    escape_markdown_v2(
                        tournament['status'])}."
            ),
            parse_mode="MarkdownV2",
        )
        return

    current_round = tournament.get("current_swiss_round", 0)
    num_total_rounds = tournament.get("num_swiss_rounds", 0)

    if current_round >= num_total_rounds:
        await reply_method(
            text=escape_markdown_v2(
                f"⚠️ All {num_total_rounds} rounds of Swiss tournament `{
                    escape_markdown_v2(t_id)}` have already been completed."
            ),
            parse_mode="MarkdownV2",
        )
        return

    # Check if all matches in the current round are completed
    remaining_matches_in_current_round = get_matches_for_tournament(
        t_id, match_status="scheduled", round_number=current_round, group_id=None
    )
    if remaining_matches_in_current_round:
        await reply_method(
            text=escape_markdown_v2(
                f"⚠️ Not all matches in Round {current_round} are completed yet. Please wait for all matches to be reported before advancing."
            ),
            parse_mode="MarkdownV2",
        )
        return

    new_round_num = current_round + 1
    registered_players = get_registered_players(t_id)

    if not registered_players or len(registered_players) < 2:
        await reply_method(
            escape_markdown_v2(
                f"⚠️ Not enough registered players to continue Swiss tournament `{
                    escape_markdown_v2(t_id)}`."
            ),
            parse_mode="MarkdownV2",
        )
        return

    # Update current_swiss_round in DB
    if not update_tournament_swiss_round(t_id, new_round_num):
        await reply_method(
            escape_markdown_v2(
                "⚠️ Failed to update current Swiss round in the database. Please try again."
            ),
            parse_mode="MarkdownV2",
        )
        return

    parts = [
        f"🎉 Advancing Swiss Tournament *{
            escape_markdown_v2(
                tournament['name'])}* to Round {
            escape_markdown_v2(
                str(new_round_num))}\\!"
    ]  # Fixed '!' escaping
    parts.append(escape_markdown_v2(
        "\n♟️ *Generating Swiss Round Matches...*"))

    # Generate matches for the new round
    swiss_matches_for_new_round = generate_swiss_round_matches(
        t_id, new_round_num, registered_players
    )
    total_matches_generated = 0

    if not swiss_matches_for_new_round:
        parts.append(
            escape_markdown_v2(
                f"⚠️ Could not generate matches for Swiss Round {new_round_num}. This might indicate an issue with player pairing or too few active players."
            )
        )
        await reply_method("\n".join(parts), parse_mode="MarkdownV2")
        return

    parts.append(
        f"\n*{escape_markdown_v2(f'--- Swiss Round {new_round_num} ---')}*")
    for m_dets in swiss_matches_for_new_round:
        m_id = add_match_to_db(m_dets)
        if m_id:
            total_matches_generated += 1
            if m_dets["status"] == "bye":
                parts.append(
                    f"  M\\-ID `{m_id}`: {
                        escape_markdown_v2(
                            m_dets['player1_username'])} gets a *BYE*"
                )
            else:
                parts.append(
                    f"  M\\-ID `{m_id}`: {
                        escape_markdown_v2(
                            m_dets['player1_username'])} vs {
                        escape_markdown_v2(
                            m_dets['player2_username'])}"
                )
                await notify_players_of_match(
                    context,
                    m_id,
                    t_id,
                    tournament["name"],
                    m_dets["player1_user_id"],
                    m_dets["player1_username"],
                    m_dets["player2_user_id"],
                    m_dets["player2_username"],
                )
        else:
            logger.error(
                f"Failed to add Swiss match to DB for T_ID {t_id}, round {new_round_num}."
            )

    if total_matches_generated > 0:
        parts.append(
            escape_markdown_v2(
                f"\nSwiss Round {new_round_num} fixtures generated successfully ({total_matches_generated} matches in total)!"
            )
        )
        parts.append(
            escape_markdown_v2(
                "Good luck to all participants! Use `/report_score <Match_ID> <your_score> <opponent_score>` to report your results."
            )
        )
        parts.append(
            escape_markdown_v2(
                f"You can view standings and matches with `/view_matches {
                    escape_markdown_v2(t_id)}`\\."
            )
        )
        public_advance_message_swiss = (
            f"📢 Swiss Tournament *{
                escape_markdown_v2(
                    tournament['name'])}* has advanced to Round {
                escape_markdown_v2(
                    str(new_round_num))}\\!\n"
            f"Matches for this round have been generated\\. Check your DMs for notifications\\! "
            f"View standings and matches with `/view_matches {
                escape_markdown_v2(t_id)}`\\."
        )
        await send_public_announcement(context, t_id, public_advance_message_swiss)
    else:
        parts.append(
            escape_markdown_v2(
                f"⚠️ No matches were generated for Swiss Round {new_round_num}. This might indicate an issue with player pairing or too few active players."
            )
        )
        if new_round_num == num_total_rounds:
            await reply_method(
                escape_markdown_v2(
                    f"⚠️ No matches generated for the final Swiss round. Tournament may end without a clear winner."
                ),
                parse_mode="MarkdownV2",
            )
            update_tournament_status(t_id, "completed")

    await reply_method("\n".join(parts), parse_mode="MarkdownV2")


def get_knockout_round_name(current_round_num: int, total_rounds: int) -> str:
    """Translates a round number into a professional name like 'Quarter-final'."""

    if total_rounds <= 0:
        return f"Round {current_round_num}"

    # The final round is always the "Final"
    if current_round_num == total_rounds:
        return "Final"

    # The second to last round is always the "Semi-final"
    if current_round_num == total_rounds - 1:
        # Avoid calling a 2-person tournament's first round a "Semi-final"
        if total_rounds > 1:
            return "Semi-final"
        else:  # This case is for a 2-person tourney, the first and only round is the Final
            return "Final"

    # The third to last round is the "Quarter-final"
    if current_round_num == total_rounds - 2:
        # Avoid calling a 4-person tournament's first round a "Quarter-final"
        if total_rounds > 2:
            return "Quarter-final"

    # For all other rounds, name them by the number of participants
    # e.g., A round with 16 participants is the "Round of 16"
    num_participants_in_round = 2 ** (total_rounds - current_round_num + 1)

    # Only use "Round of X" for 16 or more participants, otherwise it's less
    # common
    if num_participants_in_round >= 16:
        return f"Round of {num_participants_in_round}"

    # Fallback for any other case
    return f"Round {current_round_num}"


async def view_tournament_matches_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays matches for a tournament. Now admin-only in groups."""

    # Get essential objects
    chat = update.effective_chat
    user = update.effective_user
    
    # Admin-Only Check (only for the typed command)
    if update.message and chat.type in ['group', 'supergroup']:
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            if user.id not in [admin.user.id for admin in admins]:
                logger.info(f"Ignoring /view_matches from non-admin {user.id} in group {chat.id}")
                return
        except Exception as e:
            logger.error(f"Failed to check admin status for /view_matches: {e}")

    # These lines must come first to define the variables.
    args = context.args
    query = update.callback_query
    user = update.effective_user
    chat = update.effective_chat

    t_id = ""
    if args:
        t_id = args[0]
    elif query and "view_matches_cmd_" in query.data:
        t_id = query.data.split("view_matches_cmd_")[1]

    if not t_id:
        if query:
            await query.answer("Could not identify the tournament.", show_alert=True)
        elif update.message:
            await update.message.reply_text("Usage: /view_matches <Tournament_ID>")
        return

    tournament = get_tournament_details_by_id(t_id)

    if not tournament:
        error_msg = f"⚠️ Tournament with ID <code>{t_id}</code> not found."
        # ... (error handling remains the same)
        return

    display_parts = []
    title = f"<b>🏆 Details for {
        tournament.get(
            'name',
            'N/A')}</b> (ID: <code>{t_id}</code>)"
    display_parts.append(title)

    settings_block = (
        f"\n<b>Match Settings:</b>\n"
        f"  • Time: <code>{tournament.get('tournament_time', 'N/A')}</code>\n"
        f"  • Penalties: <code>{tournament.get('penalties', 'N/A')}</code>\n"
        f"  • Extra Time: <code>{tournament.get('extra_time', 'N/A')}</code>\n"
        f"  • Conditions: <code>{
            tournament.get(
                'conditions',
                'None')}</code>\n"
    )
    display_parts.append(settings_block)

    # --- Add specific content based on tournament type ---
    if tournament["type"] == "Group Stage & Knockout":
        display_parts.append("<b>--- Group Stage ---</b>")
        all_groups = get_groups_for_tournament(t_id)

        if not all_groups:
            display_parts.append(
                "\n<i>Groups will be generated when the tournament starts.</i>"
            )
        else:
            # RESTORED: Logic to display group standings tables
            for group in all_groups:
                display_parts.append(
                    f"\n<b>{group['group_name']} Standings:</b>")
                standings = get_group_stage_standings(t_id, group["group_id"])
                if not standings:
                    display_parts.append(
                        "<i>Standings will appear as matches are played.</i>"
                    )
                else:
                    team_data = [
                        {
                            "rank": i + 1,
                            "team_name": s["username"],
                            "played": s["games_played"],
                            "wins": s["wins"],
                            "draws": s["draws"],
                            "losses": s["losses"],
                            "goals_for": s["goals_for"],
                            "goals_against": s["goals_against"],
                            "goal_difference": s["goal_difference"],
                            "points": s["points"],
                        }
                        for i, s in enumerate(standings)
                    ]
                    display_parts.append(
                        f"<pre>\n{generate_league_table(team_data)}\n</pre>"
                    )

                group_matches = get_matches_for_group(t_id, group["group_id"])
                if group_matches:
                    display_parts.append(
                        f"<b>{group['group_name']} Matches:</b>")
                    for m in group_matches:
                        p1n, p2n = m.get("player1_username", "TBD"), m.get(
                            "player2_username", "TBD"
                        )
                        match_line = f"  <code>{
                            m['match_id']}</code>: {p1n} vs {p2n}"
                        if m["status"] == "completed":
                            match_line += f" | <b>{m.get('score', 'N/A')}</b>"
                        else:
                            match_line += f" | <i>{m['status']}</i>"
                        display_parts.append(match_line)

            knockout_matches = get_matches_for_tournament(t_id, group_id=None)
            if knockout_matches:
                display_parts.append("\n<b>--- Knockout Stage ---</b>")
                total_ko_rounds = (
                    max(m["round_number"] for m in knockout_matches)
                    if knockout_matches
                    else 0
                )
                ko_matches_by_round = {
                    r: [] for r in sorted({m["round_number"] for m in knockout_matches})
                }
                for m in knockout_matches:
                    ko_matches_by_round[m["round_number"]].append(m)

                for r_num, r_matches in ko_matches_by_round.items():
                    round_name = get_knockout_round_name(
                        r_num, total_ko_rounds)
                    display_parts.append(f"\n<b>{round_name}</b>")
                    for m_detail in sorted(
                        r_matches, key=lambda x: x["match_in_round_index"]
                    ):
                        p1, p2 = m_detail.get(
                            "player1_username", "<i>TBD</i>"
                        ), m_detail.get("player2_username", "<i>TBD</i>")
                        match_line = (
                            f"  <code>{
                                m_detail['match_id']}</code>: {p1} vs {p2}"
                        )
                        if m_detail["status"] == "completed":
                            match_line += f" | <b>{
                                m_detail.get(
                                    'score', 'N/A')}</b>"
                        else:
                            match_line += f" | <i>{m_detail['status']}</i>"
                        display_parts.append(match_line)

    elif tournament["type"] in ["Round Robin", "Swiss"]:
        display_parts.append(
            f"<b>--- {tournament['type']} Standings & Fixtures ---</b>"
        )
        if tournament["type"] == "Swiss":
            display_parts.append(
                f"<i>Current Round: {
                    tournament.get(
                        'current_swiss_round', 0)}/{
                    tournament.get(
                        'num_swiss_rounds', 0)}</i>"
            )

        # RESTORED: Logic to display standings table
        standings = get_round_robin_standings(t_id)
        if standings:
            team_data = [
                {
                    "rank": i + 1,
                    "team_name": s["username"],
                    "played": s["games_played"],
                    "wins": s["wins"],
                    "draws": s["draws"],
                    "losses": s["losses"],
                    "goals_for": s["goals_for"],
                    "goals_against": s["goals_against"],
                    "goal_difference": s["goal_difference"],
                    "points": s["points"],
                }
                for i, s in enumerate(standings)
            ]
            display_parts.append(
                f"<pre>\n{
                    generate_league_table(team_data)}\n</pre>")

        all_matches = get_matches_for_tournament(t_id, group_id=None)
        if not all_matches:
            display_parts.append("\nNo matches generated yet.")
        else:
            matches_by_round = {
                r: [] for r in sorted({m["round_number"] for m in all_matches})
            }
            for m in all_matches:
                matches_by_round[m["round_number"]].append(m)

            for r_num, r_matches in matches_by_round.items():
                display_parts.append(f"\n<b>Match Day {r_num}</b>")
                for m in sorted(
                        r_matches, key=lambda x: x["match_in_round_index"]):
                    p1n, p2n = m.get("player1_username", "TBD"), m.get(
                        "player2_username", "TBD"
                    )
                    match_line = f"  <code>{
                        m['match_id']}</code>: {p1n} vs {p2n}"
                    if m["status"] == "completed":
                        match_line += f" | Score: <b>{
                            m.get(
                                'score',
                                'N/A')}</b>"
                    elif m["status"] == "bye":
                        match_line = (
                            f"  <code>{
                                m['match_id']}</code>: {p1n} gets a <b>BYE</b>"
                        )
                    else:
                        match_line += f" | Status: <i>{m['status']}</i>"
                    display_parts.append(match_line)

    else:  # Single Elimination
        display_parts.append("<b>--- Bracket ---</b>")
        if tournament["status"] == "completed" and tournament.get(
                "winner_username"):
            display_parts.append(
                f"🥇 Winner: <b>{
                    tournament['winner_username']}</b>")

        all_matches = get_matches_for_tournament(t_id)
        if not all_matches:
            display_parts.append(
                "\n<i>No matches have been generated yet.</i>")
        else:
            total_rounds = (
                max(m["round_number"]
                    for m in all_matches) if all_matches else 0
            )
            matches_by_round = {}
            for m in all_matches:
                matches_by_round.setdefault(m["round_number"], []).append(m)

            for round_num in sorted(matches_by_round.keys()):
                round_name = get_knockout_round_name(round_num, total_rounds)
                display_parts.append(f"\n<b>{round_name}</b>")
                for m_detail in sorted(
                    matches_by_round[round_num], key=lambda x: x["match_in_round_index"]
                ):
                    p1 = m_detail.get("player1_username", "<i>TBD</i>")
                    p2 = m_detail.get("player2_username", "<i>TBD</i>")
                    next_match = (
                        f" ➡️ <code>{m_detail['next_match_id']}</code>"
                        if m_detail.get("next_match_id")
                        else ""
                    )
                    match_line = f"  <code>{m_detail['match_id']}</code>: "
                    if m_detail["status"] == "bye":
                        adv_player = p1 if m_detail.get(
                            "player1_user_id") else p2
                        match_line += f"{adv_player} has a <b>BYE</b>{next_match}"
                    elif m_detail["status"] == "completed":
                        winner_name = (
                            p1
                            if m_detail.get("winner_user_id")
                            == m_detail.get("player1_user_id")
                            else p2
                        )
                        match_line += f"{p1} vs {p2} | <b>{
                            m_detail.get(
                                'score', 'N/A')}</b> | 🏆 {winner_name}{next_match}"
                    else:
                        match_line += (
                            f"{p1} vs {p2} | <i>{
                                m_detail['status']}</i>{next_match}"
                        )
                    display_parts.append(match_line)

    final_message = "\n".join(display_parts)

    # --- Conditional Sending Logic (Unchanged) ---
    if query:
        is_creator = user.id == tournament["creator_id"]
        is_group_chat = chat.type in ["group", "supergroup"]

        if is_group_chat and not is_creator:
            try:
                await context.bot.send_message(
                    chat_id=user.id, text=final_message, parse_mode="HTML"
                )
                await query.answer(
                    "I've sent the details to you in a private message.",
                    show_alert=False,
                )
            except Forbidden:
                await query.answer(
                    "I can't DM you. Please start a chat with me first!",
                    show_alert=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to send match details DM to {
                        user.id}: {e}")
                await query.answer(
                    "Sorry, an error occurred while sending the DM.", show_alert=True
                )
        else:
            back_button = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Back to Tournaments", callback_data="view_tournaments"
                        )
                    ]
                ]
            )
            try:
                await query.edit_message_text(
                    text=final_message, parse_mode="HTML", reply_markup=back_button
                )
            except Exception as e:
                logger.warning(
                    f"Could not edit message (it may be identical): {e}")
                await query.answer()
    elif update.message:
        await update.message.reply_text(text=final_message, parse_mode="HTML")


async def report_score_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Allows players to report the score of a match independently."""
    user = update.effective_user
    user_id = user.id
    args = context.args
    logger.info(
        f"User {user_id} ({
            user.username or user.first_name}) initiated /report_score with args: {args}."
    )

    if len(args) != 3:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Incorrect format. Usage: `/report_score <Match_ID> <your_score> <opponent_score>`\nExample: `/report_score 123 2 1`"
            ),
            parse_mode="MarkdownV2",
        )
        return

    try:
        match_id_arg = int(args[0])
        score_user_reported = int(args[1])
        score_opponent_reported = int(args[2])
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2("⚠️ Match ID and scores must be numbers."),
            parse_mode="MarkdownV2",
        )
        return

    if score_user_reported < 0 or score_opponent_reported < 0:
        await update.message.reply_text(
            escape_markdown_v2("⚠️ Scores cannot be negative."), parse_mode="MarkdownV2"
        )
        return

    match_details = get_match_details_by_match_id(match_id_arg)
    if not match_details:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Match ID `{
                    escape_markdown_v2(str(match_id_arg))}` not found."),
            parse_mode="MarkdownV2",
        )
        return

    match_status = match_details.get("status")
    if match_status in ["completed", "bye", "cancelled", "conflict"]:
        await update.message.reply_text(
            escape_markdown_v2(
                f"Match ID `{
                    escape_markdown_v2(str(match_id_arg))
                }` is already marked as '{
                    escape_markdown_v2(match_status)}' and cannot be reported again."
            ),
            parse_mode="MarkdownV2",
        )
        return

    p1id = match_details.get("player1_user_id")
    p2id = match_details.get("player2_user_id")

    if not p1id or not p2id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ This match is incomplete (one or both players missing). Cannot report score."
            ),
            parse_mode="MarkdownV2",
        )
        return

    reporter_is_p1 = user_id == p1id
    reporter_is_p2 = user_id == p2id

    if not reporter_is_p1 and not reporter_is_p2:
        await update.message.reply_text(
            escape_markdown_v2("⚠️ You are not a participant in this match."),
            parse_mode="MarkdownV2",
        )
        return

    if reporter_is_p1:
        score_for_p1_in_match = score_user_reported
        score_for_p2_in_match = score_opponent_reported
        opponent_id = p2id
    else:  # reporter_is_p2
        score_for_p1_in_match = score_opponent_reported
        score_for_p2_in_match = score_user_reported
        opponent_id = p1id

    if not add_score_submission(
        match_id_arg, user_id, score_for_p1_in_match, score_for_p2_in_match
    ):
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Failed to record your score submission. Please try again."
            ),
            parse_mode="MarkdownV2",
        )
        return

    all_submissions = get_score_submissions_for_match(match_id_arg)

    opponent_submission = None
    for sub in all_submissions:
        if sub["user_id"] == opponent_id:
            opponent_submission = sub
            break

    tournament = get_tournament_details_by_id(match_details["tournament_id"])
    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Critical error: Tournament associated with this match not found."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if opponent_submission:
        if (
            score_for_p1_in_match == opponent_submission["score_p1"]
            and score_for_p2_in_match == opponent_submission["score_p2"]
        ):
            # Scores match, proceed to complete the match
            final_score_str = f"{score_for_p1_in_match}-{score_for_p2_in_match}"
            winner_id = None
            if score_for_p1_in_match > score_for_p2_in_match:
                winner_id = p1id
            elif score_for_p2_in_match > score_for_p1_in_match:
                winner_id = p2id

            # --- START OF CORRECTED LOGIC ---
            is_knockout_phase = (
                tournament.get("type") == "Single Elimination" or
                (tournament.get("type") == "Group Stage & Knockout" and match_details.get("group_id") is None) or
                tournament.get("status") == "ongoing_knockout"  # Correct check for Swiss KO
            )

            if winner_id is None and is_knockout_phase:
                # This code will now ONLY run for true knockout matches.
                await update.message.reply_text(
                    escape_markdown_v2(
                        "⚠️ Scores matched, but a draw is not allowed in a knockout stage match. Please contact the tournament creator to resolve this."
                    ),
                    parse_mode="MarkdownV2",
                )
                conn = sqlite3.connect(DB_NAME)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE matches SET status = 'conflict' WHERE match_id = ?",
                    (match_id_arg,),
                )
                conn.commit()
                conn.close()
                
                creator_id = tournament.get("creator_id")
                if creator_id:
                    t_name_esc = escape_markdown_v2(tournament['name'])
                    msg_to_creator = (
                        f"🚨 *ADMIN ALERT: Tie in Knockout Stage*\n\n"
                        f"Match ID `{match_id_arg}` in tournament '{t_name_esc}' was reported as a draw, which is not allowed in a knockout round.\n\n"
                        f"Please resolve it manually using:\n"
                        f"`/conflict_resolve {match_id_arg} <P1_score> <P2_score>`"
                    )
                    try:
                        await context.bot.send_message(creator_id, msg_to_creator, parse_mode="MarkdownV2")
                    except Exception as e:
                        logger.error(f"Failed to DM creator about KO tie for match {match_id_arg}: {e}")
                return
            # --- END OF CORRECTED LOGIC ---

            if await update_match_score_and_progress(
                context, match_id_arg, final_score_str, user_id, winner_id, "completed"
            ):
                clear_score_submissions_for_match(match_id_arg)
                t_name_esc = escape_markdown_v2(tournament["name"])
                p1_name_esc = escape_markdown_v2(match_details.get("player1_username", "Player 1"))
                p2_name_esc = escape_markdown_v2(match_details.get("player2_username", "Player 2"))
                
                winner_display_name_outcome = "It's a Tie\\!"
                if winner_id:
                    winner_display_name_outcome = p1_name_esc if winner_id == p1id else p2_name_esc

                response_message = (
                    f"✅ Score for match ID `{str(match_id_arg)}` in tournament '{t_name_esc}' confirmed and updated successfully\\!\n\n"
                    f"Match: {p1_name_esc} vs {p2_name_esc}\n"
                    f"Final Score \\(P1 vs P2\\): *{escape_markdown_v2(final_score_str)}*\n"
                    f"Outcome: Winner is *{winner_display_name_outcome}*\\.\n\n"
                    f"The match has been marked as completed\\. Thank you both\\!"
                )
                await update.message.reply_text(response_message, parse_mode="MarkdownV2")
                try:
                    await context.bot.send_message(opponent_id, response_message, parse_mode="MarkdownV2")
                except Exception as e:
                    logger.warning(f"Could not send DM to opponent {opponent_id} after score confirmation: {e}")
            else:
                await update.message.reply_text(
                    escape_markdown_v2("⚠️ There was an issue finalizing the match score. Please contact an admin."),
                    parse_mode="MarkdownV2",
                )
        else:
            # This is the score conflict logic (when reports don't match)
            # It is unchanged and correct.
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("UPDATE matches SET status = 'conflict' WHERE match_id = ?", (match_id_arg,))
            conn.commit()
            conn.close()

            t_name_esc = escape_markdown_v2(tournament["name"])
            p1_name_esc = escape_markdown_v2(match_details.get("player1_username", "Player 1"))
            p2_name_esc = escape_markdown_v2(match_details.get("player2_username", "Player 2"))
            reporter_username_esc = escape_markdown_v2(user.full_name or f"User_{user_id}")
            opponent_username_esc = escape_markdown_v2(get_player_username_by_id(opponent_id) or f"User_{opponent_id}")
            
            reporter_score_str = f"{score_user_reported}-{score_opponent_reported}"
            opponent_score_str = f"{opponent_submission['score_p2']}-{opponent_submission['score_p1']}" if reporter_is_p1 else f"{opponent_submission['score_p1']}-{opponent_submission['score_p2']}"

            conflict_message_to_players = (
                f"🚨 *Score Conflict Detected* for match ID `{str(match_id_arg)}` in tournament '{t_name_esc}'\\!\n\n"
                f"Match: {p1_name_esc} vs {p2_name_esc}\n"
                f"Your reported score: *{escape_markdown_v2(reporter_score_str)}*\n"
                f"{opponent_username_esc}'s reported score: *{escape_markdown_v2(opponent_score_str)}*\n\n"
                f"An admin has been notified to manually resolve this conflict\\. Please wait for their decision\\."
            )
            await update.message.reply_text(conflict_message_to_players, parse_mode="MarkdownV2")
            try:
                await context.bot.send_message(opponent_id, conflict_message_to_players, parse_mode="MarkdownV2")
            except Exception as e:
                logger.warning(f"Could not send DM to opponent {opponent_id} about score conflict: {e}")
            
            creator_id = tournament.get("creator_id")
            if creator_id:
                admin_notification = (
                    f"🚨 *ADMIN ALERT: Score Conflict* for match ID `{match_id_arg}` in tournament '{t_name_esc}'\\!\n\n"
                    f"Match: {p1_name_esc} vs {p2_name_esc}\n"
                    f"{reporter_username_esc} reported: *{escape_markdown_v2(reporter_score_str)}*\n"
                    f"{opponent_username_esc} reported: *{escape_markdown_v2(opponent_score_str)}*\n\n"
                    f"Please resolve this manually using:\n`/conflict_resolve {match_id_arg} <final_score_P1> <final_score_P2>`"
                )
                try:
                    await context.bot.send_message(creator_id, admin_notification, parse_mode="MarkdownV2")
                except Exception as e:
                    logger.error(f"Error notifying admin {creator_id} about conflict: {e}")

    else:
        # This is the logic for the first player reporting
        # It is unchanged and correct.
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("UPDATE matches SET status = 'pending_opponent_report' WHERE match_id = ?", (match_id_arg,))
        conn.commit()
        conn.close()

        t_name_esc = escape_markdown_v2(tournament["name"])
        p1_name_esc = escape_markdown_v2(match_details.get("player1_username", "Player 1"))
        p2_name_esc = escape_markdown_v2(match_details.get("player2_username", "Player 2"))
        reporter_username_esc = escape_markdown_v2(user.full_name or f"User_{user_id}")
        opponent_display_name = get_player_username_by_id(opponent_id)
        opponent_mention = f"[{escape_markdown_v2(opponent_display_name)}](tg://user?id={opponent_id})"
        
        score_log = escape_markdown_v2(f"{score_for_p1_in_match}-{score_for_p2_in_match}")
        log_message = (
            f"📝 *Score Reported* in '{t_name_esc}'\n"
            f"   Match ID: `{match_id_arg}`\n"
            f"   Reporter: {reporter_username_esc}\n"
            f"   Score \\(P1 vs P2\\): *{score_log}*\n"
            f"   Status: Waiting for opponent to confirm\\."
        )
        await send_creator_log(context, tournament["id"], log_message)

        response_to_reporter = (
            f"✅ Your score for match ID `{str(match_id_arg)}` in tournament '{t_name_esc}' has been recorded\\!\n\n"
            f"Match: {p1_name_esc} vs {p2_name_esc}\n"
            f"Your reported score \\(P1 vs P2\\): *{score_log}*\n\n"
            f"Waiting for {opponent_mention} to report their score to confirm the result\\."
        )
        await update.message.reply_text(response_to_reporter, parse_mode="MarkdownV2")

        opponent_notification = (
            f"🔔 Your opponent, {reporter_username_esc}, has reported a score for your match in tournament '{t_name_esc}'\\!\n\n"
            f"Match ID: `{str(match_id_arg)}` \\({p1_name_esc} vs {p2_name_esc}\\)\n"
            f"Their reported score \\(P1 vs P2\\): *{score_log}*\n\n"
            f"Please submit your score using: `/report_score {str(match_id_arg)} <your_score> <opponent_score>` to confirm the result\\."
        )
        try:
            await context.bot.send_message(opponent_id, opponent_notification, parse_mode="MarkdownV2")
        except Exception as e:
            logger.warning(f"Could not DM opponent {opponent_id} for match {match_id_arg}: {e}")
            await update.message.reply_text(
                f"\\(Note: Could not DM your opponent, {opponent_mention}\\. They may have blocked the bot\\.\\)",
                parse_mode="MarkdownV2",
            )



async def conflict_resolve_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Allows tournament creator to manually resolve a score conflict."""
    user = update.effective_user
    user_id = user.id
    args = context.args

    if len(args) != 3:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Incorrect format. Usage: `/conflict_resolve <Match_ID> <final_score_P1> <final_score_P2>`\nExample: `/conflict_resolve 123 2 1`"
            ),
            parse_mode="MarkdownV2",
        )
        return

    try:
        match_id_arg = int(args[0])
        final_score_p1 = int(args[1])
        final_score_p2 = int(args[2])
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2("⚠️ Match ID and scores must be numbers."),
            parse_mode="MarkdownV2",
        )
        return

    if final_score_p1 < 0 or final_score_p2 < 0:
        await update.message.reply_text(
            escape_markdown_v2("⚠️ Scores cannot be negative."), parse_mode="MarkdownV2"
        )
        return

    match_details = get_match_details_by_match_id(match_id_arg)
    if not match_details:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Match ID `{
                    str(match_id_arg)}` not found."),
            parse_mode="MarkdownV2",
        )
        return

    tournament = get_tournament_details_by_id(match_details["tournament_id"])
    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Critical error: Tournament associated with this match not found."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["creator_id"] != user_id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Only the creator of the tournament can resolve conflicts for it."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if match_details["status"] not in [
        "conflict",
        "scheduled",
        "pending_opponent_report",
    ]:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Match ID `{
                    str(match_id_arg)}` is not in a 'conflict' state (Current status: '{
                    escape_markdown_v2(
                        match_details['status'])}')."
            ),
            parse_mode="MarkdownV2",
        )
        return

    p1id = match_details.get("player1_user_id")
    p2id = match_details.get("player2_user_id")
    if not p1id or not p2id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ This match is incomplete (one or both players missing). Cannot resolve conflict."
            ),
            parse_mode="MarkdownV2",
        )
        return

    final_score_str = f"{final_score_p1}-{final_score_p2}"
    winner_id = None
    if final_score_p1 > final_score_p2:
        winner_id = p1id
    elif final_score_p2 > final_score_p1:
        winner_id = p2id

    if winner_id is None and (
        tournament.get("type") == "Single Elimination"
        or (
            tournament.get("type") in ["Group Stage & Knockout", "Swiss"]
            and match_details.get("group_id") is None
        )
    ):
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Ties are not allowed in Single Elimination or Knockout stage matches. Please provide a decisive score."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if await update_match_score_and_progress(
        context, match_id_arg, final_score_str, user_id, winner_id, "completed"
    ):
        clear_score_submissions_for_match(match_id_arg)
        t_name_esc = escape_markdown_v2(tournament["name"])
        p1_display_name_match = escape_markdown_v2(
            match_details.get("player1_username", "Player 1")
        )
        p2_display_name_match = escape_markdown_v2(
            match_details.get("player2_username", "Player 2")
        )
        winner_display_name_outcome = "It's a Tie\\!"
        if winner_id:
            winner_display_name_outcome = (
                p1_display_name_match if winner_id == p1id else p2_display_name_match
            )

        response_message_to_admin = (
            f"✅ Conflict for match ID `{
                str(match_id_arg)}` in tournament '{t_name_esc}' resolved successfully\\!\n\n"
            f"Match: {p1_display_name_match} vs {p2_display_name_match}\n"
            f"Final Score \\(P1 vs P2\\): *{
                escape_markdown_v2(final_score_str)}*\n"
            f"Outcome: Winner is *{winner_display_name_outcome}*\\.\n\n"
            f"The match has been marked as completed\\. Players have been notified\\."
        )
        await update.message.reply_text(
            response_message_to_admin, parse_mode="MarkdownV2"
        )

        player_notification_text = (
            f"✅ Match result resolved for match ID `{
                str(match_id_arg)}` in tournament '{t_name_esc}'\\!\n\n"
            f"Match: {p1_display_name_match} vs {p2_display_name_match}\n"
            f"Final Score \\(P1 vs P2\\): *{
                escape_markdown_v2(final_score_str)}*\n"
            f"Outcome: Winner is *{winner_display_name_outcome}*\\.\n\n"
            f"This match is now marked as completed\\. Thank you for your patience\\!"
        )
        for player_id in [p1id, p2id]:
            if player_id != user_id:
                try:
                    await context.bot.send_message(
                        player_id, player_notification_text, parse_mode="MarkdownV2"
                    )
                except Forbidden:
                    logger.warning(
                        f"Could not send DM to player {player_id} about conflict resolution."
                    )
                except Exception as e_notify:
                    logger.error(
                        f"Error DMing player {player_id} about conflict resolution: {e_notify}"
                    )
    else:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ There was an issue resolving the match score or progressing the tournament. Please try again."
            ),
            parse_mode="MarkdownV2",
        )


async def set_announcement_group_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Sets the current group chat as the announcement channel for a tournament."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type

    if chat_type == "private":
        await update.message.reply_text(
            escape_markdown_v2(
                "This command can only be used within a group chat that you want to set for announcements."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            escape_markdown_v2(
                "Usage: /set_announcement_group <Tournament_ID>"),
            parse_mode="MarkdownV2",
        )
        return

    tournament_id = context.args[0]
    tournament = get_tournament_details_by_id(tournament_id)

    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                f"Tournament with ID `{
                    escape_markdown_v2(tournament_id)}` not found."
            ),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["creator_id"] != user_id:
        await update.message.reply_text(
            escape_markdown_v2(
                "Only the creator of the tournament can set its announcement group."
            ),
            parse_mode="MarkdownV2",
        )
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE tournaments SET group_chat_id = ? WHERE id = ?",
            (chat_id, tournament_id),
        )
        conn.commit()
        if cursor.rowcount > 0:
            t_name_esc = escape_markdown_v2(tournament["name"])
            await update.message.reply_text(
                escape_markdown_v2(
                    f"✅ This group has now been set as the official announcement channel for tournament '{t_name_esc}' (ID: `{
                        escape_markdown_v2(tournament_id)}`)."
                ),
                parse_mode="MarkdownV2",
            )
            logger.info(
                f"Group {chat_id} set as announcement channel for T_ID {tournament_id} by creator {user_id}."
            )
        else:
            await update.message.reply_text(
                escape_markdown_v2(
                    "Could not update the tournament. Please ensure the Tournament ID is correct."
                ),
                parse_mode="MarkdownV2",
            )
    except sqlite3.Error as e:
        logger.error(
            f"Database error in set_announcement_group for T_ID {tournament_id}: {e}"
        )
        await update.message.reply_text(
            escape_markdown_v2(
                "An error occurred while trying to update the tournament settings."
            ),
            parse_mode="MarkdownV2",
        )
    finally:
        conn.close()


async def h2h_command(update: Update,
                      context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays head-to-head stats by replying to a user's message."""

    if not update.message.reply_to_message:
        await update.message.reply_text("<b>How to use:</b> Reply to a message from the user you want to compare stats with and type /h2h", parse_mode='HTML')
        return

    user1 = update.effective_user
    user2 = update.message.reply_to_message.from_user

    if user2.is_bot:
        await update.message.reply_text("You can't check head-to-head stats against a bot.")
        return

    if user1.id == user2.id:
        await update.message.reply_text("You can't compare stats with yourself. Use /mystats instead.")
        return

    # Fetch stats from the DB
    stats = get_h2h_stats_from_db(user1.id, user2.id)

    # --- Build the response message ---
    user1_name = user1.first_name
    user2_name = user2.first_name

    if stats is None:
        await update.message.reply_text(f"You and {user2_name} have never played an official match against each other.")
        return

    if not stats:
        await update.message.reply_text("Could not retrieve head-to-head stats due to an error.")
        return

    # Determine the leader
    leader_text = ""
    if stats['user1_wins'] > stats['user2_wins']:
        leader_text = f"<b>Overall Record: {user1_name} leads {
            stats['user1_wins']} - {
            stats['user2_wins']}</b>"
    elif stats['user2_wins'] > stats['user1_wins']:
        leader_text = f"<b>Overall Record: {user2_name} leads {
            stats['user2_wins']} - {
            stats['user1_wins']}</b>"
    else:
        leader_text = f"<b>Overall Record: Tied {
            stats['user1_wins']} - {
            stats['user2_wins']}</b>"

    total_matches = stats['user1_wins'] + stats['user2_wins'] + stats['draws']

    message_parts = [
        f"<b>⚔️ Head-to-Head: {user1_name} vs. {user2_name} ⚔️</b>",
        "",
        leader_text,
        "<code>--------------------</code>",
        f"• <b>Total Matches Played:</b> {total_matches}",
        f"• <b>{user1_name} Wins:</b> {stats['user1_wins']}",
        f"• <b>{user2_name} Wins:</b> {stats['user2_wins']}",
        f"• <b>Draws:</b> {stats['draws']}",
        "<code>--------------------</code>"
    ]

    if stats['recent_matches']:
        message_parts.append("\n<b>Recent Encounters:</b>")
        for match in stats['recent_matches']:
            message_parts.append(
                f"<i>({match['tournament_name']})</i>:  {user1_name} <b>{match['score']}</b> {user2_name}")

    await update.message.reply_text("\n".join(message_parts), parse_mode='HTML')


async def broadcast_command(
        update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """A command for a creator to send a message to all tournament participants."""
    user = update.effective_user

    # 1. Validate the arguments
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            escape_markdown_v2(
                "Usage: /broadcast <Tournament_ID> <Your message here...>"),
            parse_mode='MarkdownV2'
        )
        return

    tournament_id = context.args[0]
    message_text = " ".join(context.args[1:])

    # 2. Verify the tournament and that the user is the creator
    tournament = get_tournament_details_by_id(tournament_id)
    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Tournament with ID '{tournament_id}' not found."),
            parse_mode='MarkdownV2'
        )
        return

    if tournament['creator_id'] != user.id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Only the creator of this tournament can send a broadcast."),
            parse_mode='MarkdownV2'
        )
        return

    # 3. Get all registered players
    players = get_registered_players(tournament_id)
    if not players:
        await update.message.reply_text("This tournament has no registered players to broadcast to.")
        return

    # Let the creator know the broadcast is starting
    await update.message.reply_text(f"🚀 Starting broadcast to {len(players)} players. This may take a moment...")

    # 4. Loop through players and send the message
    success_count = 0
    failure_count = 0
    t_name_esc = escape_markdown_v2(tournament['name'])

    # We use the raw message text here, not escaped, so that the creator can
    # use markdown.
    broadcast_message = (
        f"📢 A message from the creator of tournament *{t_name_esc}*:\n\n"
        f"{message_text}"
    )

    for player in players:
        player_id = player.get('user_id')
        if not player_id:
            continue

        try:
            # We send the message with MarkdownV2, allowing creators to use
            # formatting.
            await context.bot.send_message(chat_id=player_id, text=broadcast_message, parse_mode='MarkdownV2')
            success_count += 1
        except Forbidden:
            logger.warning(
                f"Broadcast failed for P_ID {player_id}. Bot blocked.")
            failure_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to P_ID {player_id}: {e}")
            failure_count += 1

    # 5. Report the result back to the creator
    final_report = (
        f"✅ Broadcast complete for tournament '{tournament['name']}'.\n\n"
        f"Sent successfully to *{success_count}* players."
    )
    if failure_count > 0:
        final_report += f"\nFailed to send to *{failure_count}* players (they may have blocked the bot)."

    await update.message.reply_text(final_report, parse_mode='Markdown')


async def remind_players_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """A command for tournament creators to manually remind players of pending matches."""
    user = update.effective_user

    # 1. Validate arguments
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            escape_markdown_v2("Usage: /remind_players <Tournament_ID>"),
            parse_mode="MarkdownV2",
        )
        return

    tournament_id = context.args[0]

    # 2. Verify tournament and creator
    tournament = get_tournament_details_by_id(tournament_id)
    if not tournament:
        await update.message.reply_text(
            escape_markdown_v2(
                f"⚠️ Tournament with ID '{tournament_id}' not found."),
            parse_mode="MarkdownV2",
        )
        return

    if tournament["creator_id"] != user.id:
        await update.message.reply_text(
            escape_markdown_v2(
                "⚠️ Only the creator of the tournament can send reminders."
            ),
            parse_mode="MarkdownV2",
        )
        return

    # 3. Find all pending matches
    scheduled_matches = get_matches_for_tournament(
        tournament_id, match_status="scheduled"
    )
    pending_report_matches = get_matches_for_tournament(
        tournament_id, match_status="pending_opponent_report"
    )
    all_pending_matches = scheduled_matches + pending_report_matches

    if not all_pending_matches:
        await update.message.reply_text(
            escape_markdown_v2(
                f"ℹ️ No pending matches found for '{
                    tournament['name']}'. Everyone is up to date!"
            ),
            parse_mode="MarkdownV2",
        )
        return

    # 4. Loop through matches and send reminders
    reminders_sent_successfully = 0
    t_name_esc = escape_markdown_v2(tournament["name"])

    for match in all_pending_matches:
        p1_id = match.get("player1_user_id")
        p2_id = match.get("player2_user_id")

        if not p1_id or not p2_id:
            continue  # Skip matches with missing players

        p1_username = escape_markdown_v2(
            match.get("player1_username", f"User_{p1_id}"))
        p2_username = escape_markdown_v2(
            match.get("player2_username", f"User_{p2_id}"))
        match_id_esc = escape_markdown_v2(str(match["match_id"]))

        # Create personalized messages
        msg_to_p1 = (
            f"🔔 *Match Reminder* 🔔\n\n"
            f"The creator of the *{t_name_esc}* tournament has sent a reminder for your pending match\\.\n\n"
            f"**Match ID:** `{match_id_esc}`\n"
            f"**Your Opponent:** {p2_username}\n\n"
            f"Please coordinate to play the match and report the score\\. Thank you\\!"
        )
        msg_to_p2 = (
            f"🔔 *Match Reminder* 🔔\n\n"
            f"The creator of the *{t_name_esc}* tournament has sent a reminder for your pending match\\.\n\n"
            f"**Match ID:** `{match_id_esc}`\n"
            f"**Your Opponent:** {p1_username}\n\n"
            f"Please coordinate to play the match and report the score\\. Thank you\\!"
        )

        # Send messages safely
        try:
            await context.bot.send_message(
                chat_id=p1_id, text=msg_to_p1, parse_mode="MarkdownV2"
            )
        except Forbidden:
            logger.warning(
                f"Could not send reminder to P_ID {p1_id} for match {
                    match['match_id']}. Bot blocked."
            )
        except Exception as e:
            logger.error(f"Failed to send reminder to P_ID {p1_id}: {e}")

        try:
            await context.bot.send_message(
                chat_id=p2_id, text=msg_to_p2, parse_mode="MarkdownV2"
            )
        except Forbidden:
            logger.warning(
                f"Could not send reminder to P_ID {p2_id} for match {
                    match['match_id']}. Bot blocked."
            )
        except Exception as e:
            logger.error(f"Failed to send reminder to P_ID {p2_id}: {e}")

        reminders_sent_successfully += 1

    # 5. Report back to the creator
    await update.message.reply_text(
        escape_markdown_v2(
            f"✅ Reminders have been sent for {reminders_sent_successfully} pending match(es) in the '{
                tournament['name']}' tournament."
        ),
        parse_mode="MarkdownV2",
    )


async def player_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays player stats for any user."""

    # The Admin-Only check has been removed.

    user = update.effective_user
    
    stats = get_player_stats_from_db(user.id)
    
    if stats is None:
        await update.message.reply_text("You haven't participated in any tournaments yet. Join one to start building your legacy!")
        return
        
    if not stats:
        await update.message.reply_text("Could not retrieve your stats due to an error. Please try again later.")
        return

    message = (
        f"<b>📊 Player Stats for {user.mention_html()}</b>\n\n"
        f"<b>🏆 Lifetime Achievements:</b>\n"
        f"  - Tournaments Won: <b>{stats.get('tournaments_won', 0)}</b>\n"
        f"  - Tournaments Played: {stats.get('tournaments_played', 0)}\n\n"
        f"<b>⚔️ Overall Match Record:</b>\n"
        f"  - Matches Played: {stats.get('matches_played', 0)}\n"
        f"  - Wins: <b>{stats.get('matches_won', 0)}</b>\n"
        f"  - Losses: {stats.get('matches_lost', 0)}\n"
        f"  - Win Rate: <b>{stats.get('win_rate', 0):.1f}%</b>"
    )
    
    if stats.get('achievements'):
        message += "\n\n<b>🏅 Badges & Awards:</b>"
        for badge_text in stats['achievements']:
            message += f"\n  {badge_text}"
    
    await update.message.reply_text(message, parse_mode='HTML')


async def leaderboard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Displays the advanced global tournament winners leaderboard with full stats."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    board_message_parts = ["<b>🏆 Global Player Leaderboard</b> 🏆"]

    try:
        # Fetch all columns, including the new ones
        cursor.execute(
            """
            SELECT user_id, username, points, wins, matches_played, match_wins
            FROM leaderboard_points
            ORDER BY points DESC, wins DESC, match_wins DESC
            LIMIT 10
        """
        )
        top_players = cursor.fetchall()

        if not top_players:
            board_message_parts.append(
                "\nThe leaderboard is currently empty. Go win some tournaments!"
            )
        else:
            rank_emojis = ["🥇", "🥈", "🥉"]
            for i, player in enumerate(top_players):
                rank = rank_emojis[i] if i < len(rank_emojis) else f"{i + 1}."

                # Prepare all the stats for display
                user_mention = f"<a href='tg://user?id={
                    player['user_id']}'>{
                    player.get(
                        'username',
                        'Unknown')}</a>"
                points = player.get("points", 0)
                # 'wins' column tracks tournament wins
                trophies = player.get("wins", 0)
                match_wins = player.get("match_wins", 0)
                matches_played = player.get("matches_played", 0)
                losses = matches_played - match_wins

                # Calculate win rate safely
                win_rate = (
                    (match_wins / matches_played) *
                    100 if matches_played > 0 else 0
                )

                # Build the two-line entry for each player
                player_entry = (
                    f"\n{rank} <b>{user_mention}</b> - {points} Points\n"
                    f"    └ 🏆 {trophies} | 📈 {
                        win_rate:.1f}% Win Rate ({match_wins}W / {losses}L)"
                )
                board_message_parts.append(player_entry)

    except sqlite3.Error as e:
        logger.error(f"DB error fetching leaderboard: {e}")
        board_message_parts.append(
            "\nCould not retrieve leaderboard data at this time."
        )
    finally:
        conn.close()

    final_message = "\n".join(board_message_parts)
    await update.message.reply_text(
        final_message, parse_mode="HTML", disable_web_page_preview=True
    )


def main() -> None:
    """Main function to run the bot."""
    if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
        print("CRITICAL ERROR: Bot token (BOT_TOKEN) is not set in the script!")
        logger.critical("Bot token not set! Exiting.")
        return

    init_db()
    application = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(
                create_tournament_start, pattern="^create_tournament$"
            ),
            CommandHandler("create", create_tournament_start),
        ],
        states={
            ASK_TOURNAMENT_NAME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    get_tournament_name)
            ],
            ASK_GAME_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_game_name)
            ],
            ASK_PARTICIPANT_COUNT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    get_participant_count)
            ],
            ASK_TOURNAMENT_TYPE: [
                CallbackQueryHandler(
                    get_tournament_type,
                    pattern="^(single_elimination|round_robin|group_knockout|swiss)$",
                )
            ],
            ASK_NUM_GROUPS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_num_groups)
            ],
            ASK_SWISS_ROUNDS: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    get_swiss_rounds)
            ],
            ASK_SWISS_KNOCKOUT_QUALIFIERS: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, get_swiss_knockout_qualifiers
                )
            ],  # NEW STATE
            ASK_TOURNAMENT_TIME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND,
                    get_tournament_time)
            ],
            ASK_PENALTIES: [
                CallbackQueryHandler(get_penalties, pattern="^(pk_on|pk_off)$")
            ],
            ASK_EXTRA_TIME: [
                CallbackQueryHandler(
                    get_extra_time, pattern="^(et_on|et_off)$")
            ],
            ASK_CONDITIONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_conditions)
            ],
            CONFIRM_SAVE_TOURNAMENT: [
                CallbackQueryHandler(
                    handle_final_confirmation,
                    pattern="^(confirm_save_tournament|edit_tournament_details|cancel_final_confirmation)$",
                )
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation),
            CallbackQueryHandler(
                cancel_conversation, pattern="^cancel_final_confirmation$"
            ),
        ],
        map_to_parent={ConversationHandler.END: ConversationHandler.END},
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(
        CommandHandler("view_tournaments", view_tournaments_handler)
    )
    application.add_handler(CommandHandler("help", help_command_text))
    application.add_handler(
        CommandHandler("start_tournament", start_tournament_command)
    )
    application.add_handler(
        CommandHandler("advance_swiss_round", advance_swiss_round_command)
    )
    application.add_handler(
        CommandHandler("view_matches", view_tournament_matches_command)
    )
    application.add_handler(
        CommandHandler(
            "report_score",
            report_score_command))
    application.add_handler(
        CommandHandler("conflict_resolve", conflict_resolve_command)
    )
    application.add_handler(
        CommandHandler(
            "set_announcement_group",
            set_announcement_group_command)
    )
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    application.add_handler(
        CommandHandler(
            "remind_players",
            remind_players_command))
    application.add_handler(CommandHandler("mystats", player_stats_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("award_badge", award_badge_command))
    application.add_handler(CommandHandler("h2h", h2h_command))
    application.add_handler(
        CommandHandler(
            "matchhistory",
            match_history_command))
    application.add_handler(CommandHandler("add_player", add_player_command))

    application.add_handler(
        CallbackQueryHandler(
            view_tournaments_handler,
            pattern="^view_tournaments$")
    )
    application.add_handler(
        CallbackQueryHandler(help_command_text, pattern="^help_menu$")
    )
    application.add_handler(
        CallbackQueryHandler(
            handle_join_tournament,
            pattern=r"^join_tournament_")
    )
    application.add_handler(
        CallbackQueryHandler(
            view_tournament_matches_command, pattern=r"^view_matches_cmd_"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            advance_swiss_round_command, pattern=r"^advance_swiss_round_"
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            match_history_callback, pattern=r'^mh_page_'
        )
    )

    application.add_error_handler(error_handler)

# This is the new web server code to keep the bot alive
app = Flask('')

@app.route('/')
def home():
    return "Bot is alive!"

def run_flask():
  app.run(host='0.0.0.0', port=8080)

def keep_alive():
  t = Thread(target=run_flask)
  t.start()
# End of new web server code

def main() -> None:
    """Main function to run the bot."""
    if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
        print("CRITICAL ERROR: Bot token (BOT_TOKEN) is not set in the script!")
        logger.critical("Bot token not set! Exiting.")
        return

    init_db()
    # ... (all your application.add_handler lines are here) ...
    # ... (make sure they are still inside the main() function) ...

    logger.info("Bot is starting polling...")
    print("Bot is starting polling...")
    application.run_polling()
    logger.info("Bot has stopped.")
    print("Bot has stopped.")


if __name__ == "__main__":
    keep_alive()  # This starts the web server
    main()        # This starts your bot
