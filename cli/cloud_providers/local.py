from pathlib import Path

from server.lib.dbt_server import DBTServer
from server.lib.logger import get_logger


def deploy(port: int, log_level: str = "INFO"):
    Path("./dbt-server-volume").mkdir(parents=True, exist_ok=True)
    logger = get_logger(log_level)
    dbt_server = DBTServer(logger, port, storage_backend=None, schedule_backend=None)
    dbt_server.start(reload=True)
