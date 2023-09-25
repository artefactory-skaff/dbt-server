from dbt_server.config import Settings
from dbt_server.lib.logger import DbtLogger


settings = Settings()
LOGGER = DbtLogger(settings.logging_service, settings.uuid)
