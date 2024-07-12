from dbtr.server.lib.scheduler.base import BaseScheduler
from dbtr.server.lib.logger import get_logger


class LocalScheduler(BaseScheduler):
    def create_or_update_job(self, name: str, cron_expression: str, trigger_url: str, description: str = ""):
        get_logger(level="INFO").info(f"Job {name} can be triggered at {trigger_url} (no cron have been created locally)")

    def delete(self, name: str):
        get_logger(level="INFO").info(f"Job {name} can be deleted locally")

    def list(self):
        get_logger(level="INFO").info(f"Listing jobs locally")
