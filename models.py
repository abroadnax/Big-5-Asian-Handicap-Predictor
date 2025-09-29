from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class League(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(64), unique=True, nullable=False)
    name = db.Column(db.String(128), nullable=False)

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False, index=True)
    league_id = db.Column(db.Integer, db.ForeignKey("league.id"), nullable=False)

class Match(db.Model):
    id = db.Column(db.String(128), primary_key=True)  # league+date+teams
    league_id = db.Column(db.Integer, db.ForeignKey("league.id"), nullable=False)
    home_team_id = db.Column(db.Integer, db.ForeignKey("team.id"), nullable=False)
    away_team_id = db.Column(db.Integer, db.ForeignKey("team.id"), nullable=False)
    kickoff_utc = db.Column(db.DateTime, nullable=False, index=True)
    status = db.Column(db.String(16), default="scheduled")  # scheduled/live/finished

class Prediction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(db.String(128), db.ForeignKey("match.id"), index=True)
    model = db.Column(db.String(64), nullable=False)
    ah_line = db.Column(db.Float, nullable=False)              # home-side line (negative favors home)
    p_home_cover = db.Column(db.Float, nullable=False, default=0.5)   # 0..1 (placeholder if unknown)
    p_away_cover = db.Column(db.Float, nullable=False, default=0.5)
    fair_home_decimal = db.Column(db.Float, nullable=False, default=0.0)
    fair_away_decimal = db.Column(db.Float, nullable=False, default=0.0)
    edge_home = db.Column(db.Float, nullable=False, default=0.0)      # EV (decimal*prob - 1)
    edge_away = db.Column(db.Float, nullable=False, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
