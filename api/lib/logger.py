import logging
from logging import Logger
import os

from lib.state import State


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
        self.state = State(self._uuid)

    def log(self, severity: str, new_log: str):
        log_level = get_log_level(severity)
        self.logger.log(msg=new_log, level=log_level)
        if hasattr(self, "state"):
            self.state.log(severity.upper(), new_log)


def init_logger(service: Optional[str]) -> Logger:
    logger = logging.getLogger(__name__)
    if service == "GoogleCloudLogging":
        import google.cloud.logging
        from google.cloud.logging.handlers import CloudLoggingHandler

        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client)
        logger.addHandler(handler)
    elif service == "AzureMonitor":
        from opencensus.ext.azure.log_exporter import AzureLogHandler

        logger.addHandler(AzureLogHandler())
    return logger


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
