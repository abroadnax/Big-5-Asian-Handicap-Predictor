from datetime import datetime
from flask import Flask, render_template, jsonify, request, abort
from sqlalchemy.orm import aliased

from models import db, League, Team, Match, Prediction
from config import Config


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)

    @app.route("/")
    def index():
        # Alias team table twice (home/away)
        HomeTeam = aliased(Team, name="home_t")
        AwayTeam = aliased(Team, name="away_t")

        # Include all matches dated today or later (we only store dates, not real kickoff times)
        midnight_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

        # Base match query (ordered by league, then date)
        matches = (
            db.session.query(Match, League, HomeTeam, AwayTeam)
            .join(League, League.id == Match.league_id)
            .join(HomeTeam, HomeTeam.id == Match.home_team_id)
            .join(AwayTeam, AwayTeam.id == Match.away_team_id)
            .filter(Match.kickoff_utc >= midnight_utc)
            .order_by(League.name.asc(), Match.kickoff_utc.asc())
            .all()
        )

        # Fetch all predictions for those matches at once
        match_ids = [m.id for (m, _, _, _) in matches]
        preds = (
            db.session.query(Prediction)
            .filter(Prediction.match_id.in_(match_ids))
            .all()
        )

        # Map predictions per match_id -> {model_name: ah_line}
        by_match = {}
        for p in preds:
            d = by_match.setdefault(p.match_id, {})
            d[p.model] = p.ah_line

        # Group rows by league FULL NAME
        grouped = {}  # {league_name: [rows]}
        for m, lg, ht, at in matches:
            pmap = by_match.get(m.id, {})
            grouped.setdefault(lg.name, []).append({
                "kickoff": m.kickoff_utc,
                "home": ht.name,
                "away": at.name,
                "bp_ag": pmap.get("BP-ag"),
                "bp_xg": pmap.get("BP-xg"),
                "wb_ag": pmap.get("WB-ag"),
                "wb_xg": pmap.get("WB-xg"),
                "avg":   pmap.get("BP+WB Avg"),
            })

        return render_template("index.html", grouped=grouped)

    @app.route("/leagues/<league_code>")
    def league_page(league_code):
        lg = League.query.filter_by(code=league_code).first()
        if not lg:
            abort(404)

        HomeTeam = aliased(Team, name="home_t")
        AwayTeam = aliased(Team, name="away_t")

        rows = (
            db.session.query(Match, HomeTeam, AwayTeam)
            .join(HomeTeam, HomeTeam.id == Match.home_team_id)
            .join(AwayTeam, AwayTeam.id == Match.away_team_id)
            .filter(Match.league_id == lg.id)
            .order_by(Match.kickoff_utc.asc())
            .all()
        )

        match_ids = [m.id for (m, _, _) in rows]
        preds = db.session.query(Prediction).filter(Prediction.match_id.in_(match_ids)).all()
        by_match = {}
        for p in preds:
            d = by_match.setdefault(p.match_id, {})
            d[p.model] = p.ah_line

        shaped = []
        for m, ht, at in rows:
            pmap = by_match.get(m.id, {})
            shaped.append({
                "m": m,
                "home": ht.name,
                "away": at.name,
                "bp_ag": pmap.get("BP-ag"),
                "bp_xg": pmap.get("BP-xg"),
                "wb_ag": pmap.get("WB-ag"),
                "wb_xg": pmap.get("WB-xg"),
                "avg":   pmap.get("BP+WB Avg"),
            })

        return render_template("league.html", league=lg, rows=shaped)

    @app.route("/matches/<match_id>")
    def match_page(match_id):
        HomeTeam = aliased(Team, name="home_t")
        AwayTeam = aliased(Team, name="away_t")

        row = (
            db.session.query(Match, League, HomeTeam, AwayTeam)
            .join(League, League.id == Match.league_id)
            .join(HomeTeam, HomeTeam.id == Match.home_team_id)
            .join(AwayTeam, AwayTeam.id == Match.away_team_id)
            .filter(Match.id == match_id)
            .first()
        )
        if not row:
            abort(404)

        m, lg, ht, at = row
        predictions = db.session.query(Prediction).filter_by(match_id=match_id).all()

        return render_template(
            "match.html",
            match=m, league=lg, home=ht.name, away=at.name, predictions=predictions
        )

    @app.route("/api/predictions")
    def api_predictions():
        league_code = request.args.get("league")

        q = (
            db.session.query(Prediction, Match, League)
            .join(Match, Match.id == Prediction.match_id)
            .join(League, League.id == Match.league_id)
        )
        if league_code:
            q = q.filter(League.code == league_code)

        out = []
        for p, m, lg in q.all():
            out.append({
                "match_id": m.id,
                "kickoff_utc": m.kickoff_utc.isoformat(),
                "league_code": lg.code,
                "league_name": lg.name,
                "model": p.model,
                "ah_line": p.ah_line,
            })
        return jsonify(out)

    return app


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        db.create_all()
    app.run(debug=True)
