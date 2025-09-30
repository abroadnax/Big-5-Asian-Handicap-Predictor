# pipeline/predictor.py
from __future__ import annotations

from datetime import date
import pandas as pd
import numpy as np

# Import only the classic models to avoid Aesara/BLAS issues
from penaltyblog import models as pb
import soccerdata as sd

# ---------------------- CONFIG ----------------------
# Edit these if you like (or override via env/app)
PRE_LEAGUES = ["Big 5 European Leagues Combined"]
SEASONS = [2024, 2025, 2026]

# Window controls
FORECAST_DAYS = 4        # how far ahead to include
START_DAYS_BACK = 1      # include this many days *before* today (1 keeps yesterday)
DC_RHO = 0.00175         # Dixon–Coles time-decay parameter
# ----------------------------------------------------

# tz-naive UTC midnights (safe for comparisons with datetime64[ns])
TODAY_UTC = pd.Timestamp(pd.Timestamp.utcnow().date())
START_UTC = TODAY_UTC - pd.Timedelta(days=START_DAYS_BACK)
END_UTC   = TODAY_UTC + pd.Timedelta(days=FORECAST_DAYS)



def _clean_schedule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FBref schedule into a consistent schema.
    Keeps Date/League/Home/Away plus xG columns and parsed final score.
    """
    keep_cols = [
        "league", "season", "date",
        "home_team", "home_xg", "score", "away_xg", "away_team"
    ]
    df = (
        df.reset_index()
          .loc[:, keep_cols]
          .rename(columns={
              "date": "Date",
              "home_team": "Home",
              "away_team": "Away",
              "home_xg": "xG",
              "away_xg": "xG.1",
              "score": "Score",
              "league": "League",
              "season": "Season",
          })
    )

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Parse "a-b" / "a–b" into numeric goals
    score_split = (
        df["Score"].astype(str)
        .str.replace("-", "–", regex=False)
        .str.split("–", expand=True)
    )
    df["Home Goals"] = pd.to_numeric(score_split[0], errors="coerce")
    df["Away Goals"] = pd.to_numeric(score_split[1], errors="coerce")

    for c in ("xG", "xG.1"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _predict_frame(model, upcoming: pd.DataFrame, kind: str, use_xg: bool) -> pd.DataFrame:
    """
    Predict ONLY home/away goal expectations for upcoming fixtures.
    Returns columns:
      League, Date, Home, Away, <kind>_home_<xG|ag>, <kind>_away_<xG|ag>
    """
    if model is None or upcoming.empty:
        return pd.DataFrame()

    rows = []
    for _, r in upcoming.iterrows():
        pred = model.predict(r["Home"], r["Away"])
        rows.append({
            "League": r["League"],
            "Date":   r["Date"],
            "Home":   r["Home"],
            "Away":   r["Away"],
            f"{kind}_home_{'xG' if use_xg else 'ag'}": float(pred.home_goal_expectation),
            f"{kind}_away_{'xG' if use_xg else 'ag'}": float(pred.away_goal_expectation),
        })
    return pd.DataFrame(rows)


def _fit_and_predict(historical: pd.DataFrame, upcoming: pd.DataFrame):
    """
    Fit BP/WB models on:
      - Actual Goals (with Dixon–Coles weights)
      - Expected Goals (xG) when available
    Then compute per-match handicaps (AwayExp − HomeExp) and an overall average.
    """
    # XG-based models (fit only if xG present)
    hist_xg = historical.dropna(subset=["xG", "xG.1"]).copy()
    if not hist_xg.empty:
        bp_xg = pb.BivariatePoissonGoalModel(
            hist_xg["xG"], hist_xg["xG.1"], hist_xg["Home"], hist_xg["Away"]
        )
        bp_xg.fit()
        wb_xg = pb.WeibullCopulaGoalsModel(
            hist_xg["xG"], hist_xg["xG.1"], hist_xg["Home"], hist_xg["Away"]
        )
        wb_xg.fit()
    else:
        bp_xg = wb_xg = None

    # Actual-goals models with Dixon–Coles time decay
    w = pb.dixon_coles_weights(historical["Date"], DC_RHO)
    bp_ag = pb.BivariatePoissonGoalModel(
        historical["Home Goals"], historical["Away Goals"],
        historical["Home"], historical["Away"], w
    )
    bp_ag.fit()
    wb_ag = pb.WeibullCopulaGoalsModel(
        historical["Home Goals"], historical["Away Goals"],
        historical["Home"], historical["Away"], w
    )
    wb_ag.fit()

    # Predictions: only expectations
    df_bp_xg = _predict_frame(bp_xg, upcoming, "bp", True)
    df_wb_xg = _predict_frame(wb_xg, upcoming, "wb", True)
    df_bp_ag = _predict_frame(bp_ag, upcoming, "bp", False)
    df_wb_ag = _predict_frame(wb_ag, upcoming, "wb", False)

    # Merge expectations and compute handicaps + average
    combined = upcoming[["League", "Date", "Home", "Away"]].copy()

    for df_src, h_home, h_away, alias in [
        (df_bp_ag, "bp_home_ag", "bp_away_ag", "bp_home_handicap_ag"),
        (df_bp_xg, "bp_home_xG", "bp_away_xG", "bp_home_handicap_xg"),
        (df_wb_ag, "wb_home_ag", "wb_away_ag", "wb_home_handicap_ag"),
        (df_wb_xg, "wb_home_xG", "wb_away_xG", "wb_home_handicap_xg"),
    ]:
        if not df_src.empty:
            # df_src columns: League, Date, Home, Away, <home_exp>, <away_exp>
            tmp = df_src.rename(columns={
                df_src.columns[4]: h_home,
                df_src.columns[5]: h_away
            })
            tmp[alias] = tmp[h_away] - tmp[h_home]  # AH = AwayExp − HomeExp
            combined = combined.merge(
                tmp[["League", "Date", "Home", "Away", alias]],
                on=["League", "Date", "Home", "Away"],
                how="left",
            )

    combined["Average Handicap"] = combined[[
        "bp_home_handicap_ag",
        "bp_home_handicap_xg",
        "wb_home_handicap_ag",
        "wb_home_handicap_xg",
    ]].mean(axis=1, skipna=True)

    return {
        "bp_xg": df_bp_xg,
        "wb_xg": df_wb_xg,
        "bp_ag": df_bp_ag,
        "wb_ag": df_wb_ag,
        "combined": combined,
    }


def run_for_leagues(leagues: list[str], seasons: list[int]):
    """
    Pull schedules via soccerdata.FBref, split into historical vs upcoming
    using DATE-ONLY comparisons (UTC-normalized) so near-boundary games aren’t skipped.
    """
    results = {}
    fb = sd.FBref(leagues=leagues, seasons=seasons)
    sched = fb.read_schedule()

    for lg, df_lg in sched.groupby(level=0):
        df = _clean_schedule(df_lg)

        # Normalize to date-only (UTC midnight), no tz
        date_only = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()

        # Historical: strictly before START_UTC and has final score
        historical = df[(date_only < START_UTC) & df["Score"].notna()].copy()

        # Upcoming: START_UTC … END_UTC (inclusive)
        upcoming_mask = (date_only >= START_UTC) & (date_only <= END_UTC)
        upcoming = df[upcoming_mask].copy()

        if upcoming.empty:
            results[lg] = {k: pd.DataFrame() for k in ["bp_xg", "wb_xg", "bp_ag", "wb_ag", "combined"]}
            continue

        results[lg] = _fit_and_predict(historical, upcoming)

    return results
