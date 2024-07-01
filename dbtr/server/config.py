import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CONFIG:
    port: int = int(os.getenv("PORT", 8000))
    provider: str = os.getenv("PROVIDER")
    dbt_adapter: str = os.getenv("DBT_ADAPTER")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    persisted_dir: Path = Path(__file__).parent.parent.parent / "dbt-server-volume"
    db_connection_string: str = f"sqlite:///{persisted_dir / 'db.sqlite3'}"
