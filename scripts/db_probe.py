import os, re
from sqlalchemy import create_engine, text

u = os.getenv("DATABASE_URL", "")
assert u, "DATABASE_URL missing"
print("DB:", re.sub(r"://([^:]+):([^@]+)@", "://***:***@", u))

eng = create_engine(u, pool_pre_ping=True)
with eng.begin() as con:
    total, min_dt, max_dt, next7 = con.execute(text("""
        SELECT COUNT(*),
               MIN(match_date),
               MAX(match_date),
               COUNT(*) FILTER (
                 WHERE match_date::date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
               )
        FROM predictions
    """)).one()
print(f"predictions total={total} min={min_dt} max={max_dt} upcoming7={next7}")
