from app import create_app
from models import db, League, Team, Match, Prediction
from pipeline.predictor import run_for_leagues, PRE_LEAGUES, SEASONS
from datetime import datetime

app = create_app()

def upsert_team(league_id, name):
    t = Team.query.filter_by(league_id=league_id, name=name).first()
    if not t:
        t = Team(league_id=league_id, name=name); db.session.add(t); db.session.flush()
    return t

def make_match_id(league, date_dt, home, away):
    return f"{league}-{date_dt.strftime('%Y%m%d')}-{home}-{away}".replace(' ', '_')

def refresh():
    results = run_for_leagues(PRE_LEAGUES, SEASONS)  # pulls fixtures + runs models
    for league_code, blocks in results.items():
        combined = blocks.get("combined")
        if combined is None or combined.empty:
            continue

        # Upsert league
        lg = League.query.filter_by(code=league_code).first()
        if not lg:
            lg = League(code=league_code, name=league_code)
            db.session.add(lg); db.session.flush()

        for _, row in combined.iterrows():
            date_dt = row["Date"].to_pydatetime() if hasattr(row["Date"], "to_pydatetime") else row["Date"]
            home = row["Home"]; away = row["Away"]
            mid = make_match_id(league_code, date_dt, home, away)

            # Upsert teams and match
            home_t = upsert_team(lg.id, home)
            away_t = upsert_team(lg.id, away)
            m = Match.query.get(mid)
            if not m:
                m = Match(id=mid, league_id=lg.id, home_team_id=home_t.id, away_team_id=away_t.id, kickoff_utc=date_dt)
                db.session.add(m)

            # Pull per-model handicaps (NaN -> 0.0 fallback)
            def nz(val): 
                try:
                    return float(val) if val == val else 0.0
                except Exception:
                    return 0.0

            ah_bp_ag = nz(row.get("bp_home_handicap_ag"))
            ah_bp_xg = nz(row.get("bp_home_handicap_xg"))
            ah_wb_ag = nz(row.get("wb_home_handicap_ag"))
            ah_wb_xg = nz(row.get("wb_home_handicap_xg"))
            ah_avg   = nz(row.get("Average Handicap"))

            # Store one Prediction row per model + the average
            for model_name, ah in [
                ("BP-ag", ah_bp_ag),
                ("BP-xg", ah_bp_xg),
                ("WB-ag", ah_wb_ag),
                ("WB-xg", ah_wb_xg),
                ("BP+WB Avg", ah_avg),
            ]:
                db.session.add(Prediction(
                    match_id=mid,
                    model=model_name,
                    ah_line=ah,
                    # keep placeholders for the unused fields
                    p_home_cover=0.5, p_away_cover=0.5,
                    fair_home_decimal=0.0, fair_away_decimal=0.0,
                    edge_home=0.0, edge_away=0.0
                ))

    db.session.commit()

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        refresh()
        print("Refresh complete.")
