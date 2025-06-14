# backend/db_utils.py
import os
from sqlalchemy import create_engine, Column, Integer, String, Text, TIMESTAMP, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy.sql import func
from contextlib import contextmanager

# --- Database Setup (PostgreSQL) ---
# Get the database URL from Render's environment variables
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

@contextmanager
def get_db():
    """Provides a database session for a request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- SQLAlchemy Models (Replaces your CREATE TABLE statements) ---
class Tournament(Base):
    __tablename__ = 'tournaments'
    id = Column(String(8), primary_key=True)
    creator_id = Column(Integer, nullable=False)
    name = Column(String(100), nullable=False)
    game = Column(String(50))
    status = Column(String(50), default='pending')
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    
    # Relationship to registrations
    registrations = relationship("Registration", back_populates="tournament")

class Registration(Base):
    __tablename__ = 'registrations'
    registration_id = Column(Integer, primary_key=True, autoincrement=True)
    tournament_id = Column(String(8), ForeignKey('tournaments.id'))
    user_id = Column(Integer, nullable=False)
    username = Column(String(100))
    
    # Relationship to tournament
    tournament = relationship("Tournament", back_populates="registrations")


# --- Database Functions (Refactored to use SQLAlchemy) ---
def init_db():
    """Creates all tables in the database."""
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")

def get_tournaments():
    """Fetches a list of all tournaments."""
    with get_db() as db:
        tournaments = db.query(Tournament).order_by(Tournament.created_at.desc()).all()
        # Convert to a list of dictionaries
        return [
            {"id": t.id, "name": t.name, "game": t.game, "status": t.status}
            for t in tournaments
        ]

def get_tournament_details(tournament_id: str):
    """Fetches full details for a single tournament, including players."""
    with get_db() as db:
        tournament = db.query(Tournament).filter(Tournament.id == tournament_id).first()
        if not tournament:
            return None
        
        # Convert the main tournament object to a dict
        details = {
            "id": tournament.id,
            "name": tournament.name,
            "game": tournament.game,
            "status": tournament.status,
            "creator_id": tournament.creator_id
        }
        
        # Get registered players
        players = [
            {"user_id": reg.user_id, "username": reg.username}
            for reg in tournament.registrations
        ]
        details["players"] = players
        
        return details

def add_registration(tournament_id: str, user_id: int, username: str):
    """Adds a player to a tournament."""
    with get_db() as db:
        # Check if already registered
        existing_reg = db.query(Registration).filter(
            Registration.tournament_id == tournament_id,
            Registration.user_id == user_id
        ).first()
        
        if existing_reg:
            return {"success": False, "message": "Already registered"}

        new_registration = Registration(
            tournament_id=tournament_id,
            user_id=user_id,
            username=username
        )
        db.add(new_registration)
        db.commit()
        return {"success": True, "message": "Successfully registered"}

