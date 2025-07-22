from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]   # .../PREDUN
DBT_PROJECT_DIR = ROOT_DIR / "predun_dbt"        # .../PREDUN/predun_dbt
DBT_PROFILES_DIR = Path.home() / ".dbt"

PG_URI_ENV = "PG_URI"        # export PG_URI="postgresql://user:pwd@host:port/db"
DEFAULT_CHUNK_SIZE = 250_000