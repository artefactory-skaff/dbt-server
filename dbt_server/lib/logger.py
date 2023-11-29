import logging
from logging import Logger
import os
# https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.resource import Resource
from google.cloud.logging_v2.handlers._monitored_resources import retrieve_metadata_server, _REGION_ID, _PROJECT_NAME

from dbt_server.lib.state import State


class DbtLogger:

    def __init__(self, server: bool = False):
        self.local = bool(os.getenv("LOCAL", False))
        self.server = server

        self._state: State = None

        self.logging_client = Client()
        self.logger = self.init_logger()
        self.logger.info(f"Initialized logger")

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state: State):
        self._state = new_state

    def log(self, severity: str, new_log: str):
        log_level = get_log_level(severity)
        self.logger.log(level=log_level, msg=new_log)

        if self._state is not None:
            self.state.log(severity.upper(), new_log)


    def init_logger(self) -> Logger:
        _addGcloudLoggingLevel()
        logger = logging.getLogger(__name__)

        if not self.local:
            if self.logging_client is None:
                raise Exception("No Cloud Logging client given and not running locally")
            if self.server:
                handler = server_cloud_handler(self.logging_client)
            else:
                handler = job_cloud_handler(self.logging_client)
            logger.addHandler(handler)

        return logger


def server_cloud_handler(logging_client: Client):

    region = retrieve_metadata_server(_REGION_ID)
    project = retrieve_metadata_server(_PROJECT_NAME)

    cr_job_resource = Resource(
        type="cloud_run_revision",
        labels={
            "location":  region.split("/")[-1] if region else "",
            "project_id": project,
        }
    )
    handler = CloudLoggingHandler(logging_client, resource=cr_job_resource)

    return handler


def job_cloud_handler(logging_client: Client):

    region = retrieve_metadata_server(_REGION_ID)
    project = retrieve_metadata_server(_PROJECT_NAME)
    uuid = os.environ.get("UUID")
    job_name = os.environ.get('CLOUD_RUN_JOB', 'unknownJobId')

    cr_job_resource = Resource(
        type="cloud_run_job",
        labels={
            "job_name": job_name,
            "location":  region.split("/")[-1] if region else "",
            "project_id": project,
            "uuid": uuid,
        }
    )
    labels = {"uuid": uuid}
    handler = CloudLoggingHandler(logging_client, resource=cr_job_resource, labels=labels)

    return handler


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


def _addGcloudLoggingLevel():
    _addLoggingLevel('DEFAULT', 1)
    _addLoggingLevel('NOTICE', 15)
    _addLoggingLevel('ALERT', 60)
    _addLoggingLevel('EMERGENCY', 100)

    format = '{"severity": "%(levelname)s", "message": "%(message)s",\
"sourceLocation":{"file":"%(filename)s","line":%(lineno)d,"function":"%(funcName)s"}}'
    logging.basicConfig(level=logging.DEFAULT, format=format)  # type: ignore


def _addLoggingLevel(levelName, levelNum, methodName=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    addLoggingLevel('TRACE', logging.DEBUG - 5)
    logging.getLogger(__name__).setLevel("TRACE")
    logging.getLogger(__name__).trace('that worked')
    logging.trace('so did this')
    logging.TRACE
    """
    if not methodName:
        methodName = levelName.lower()

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)
