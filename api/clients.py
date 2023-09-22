from api.config import Settings
from api.lib.logger import DbtLogger


settings = Settings()
LOGGER = DbtLogger(settings.logging_service, settings.uuid)
