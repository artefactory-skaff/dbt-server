import logging
from logging import Logger
from typing import Optional

try:
    from google.cloud.logging import Client
    from google.cloud.logging.handlers import CloudLoggingHandler
except ImportError:
    Client = None
    CloudLoggingHandler = None
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
except ImportError:
    AzureLogHandler = None

from dbt_server.lib.state import State
from dbt_server.lib.metadata_document import MetadataDocumentFactory
from dbt_server.config import Settings


settings = Settings()


class DbtLogger:
    def __init__(self, service: Optional[str] = None, uuid: Optional[str] = None):
        self.logger = init_logger(service)
        self.uuid = uuid

    @property
    def uuid(self):
        return self._uuid

    @uuid.setter
    def uuid(self, new_uuid: str):
        self._uuid = new_uuid
        metadata_document = MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, self._uuid
        )
        self.state = State(self._uuid, metadata_document)

    def log(self, severity: str, new_log: str):
        log_level = get_log_level(severity)
        self.logger.log(msg=new_log, level=log_level)
        if hasattr(self, "state"):
            self.state.log(severity.upper(), new_log)


def init_logger(service: Optional[str]) -> Logger:
    logger = logging.getLogger(__name__)
    if service == "GoogleCloudLogging":
        client = Client()
        handler = CloudLoggingHandler(client)
        logger.addHandler(handler)
        return logger
    elif service == "AzureMonitor":
        logger.addHandler(AzureLogHandler(connection_string=settings.azure.applicationinsights_connection_string))
        return logger
    elif service == "Local":
        return logger
    else:
        raise ValueError("Invalid logging type")


def get_log_level(severity: str):
    level_dict = {
        "DEFAULT": 0,
        "DEBUG": 10,
        "INFO": 20,
        "NOTICE": 20,
        "WARN": 30,
        "ERROR": 40,
        "CRITICAL": 50,
        "ALERT": 50,
        "EMERGENCY": 50,
    }
    if severity.upper() in level_dict.keys():
        return level_dict[severity.upper()]
    else:
        raise Exception(f"Unknown severity: {severity}")


LOGGER = DbtLogger(settings.logging_service, settings.uuid)
