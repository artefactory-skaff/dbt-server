import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CONFIG:
    port: int = os.getenv("PORT", 8000)
    provider: str = os.getenv("PROVIDER", "GCP")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    persisted_dir: Path = Path(__file__).parent.parent / "dbt-server-volume"
