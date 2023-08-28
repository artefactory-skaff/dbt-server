import logging
# https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
import google.cloud.logging
from google.cloud.logging import Logger
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.resource import Resource
from google.cloud.logging_v2.handlers._monitored_resources import retrieve_metadata_server, _REGION_ID, _PROJECT_NAME
import os

from state import State


class DbtLogger:

    def __init__(self, local: bool = False, server: bool = False):
        self.logger = init_logger(local, server)

    @property
    def uuid(self):
        return self._uuid

    @uuid.setter
    def uuid(self, new_uuid: str):
        self._uuid = new_uuid
        self.state = State(self._uuid)

    def log(self, severity: str, new_log: str):
        match (severity.upper()):
            case "DEBUG":
                self.logger.debug(new_log)
            case "INFO":
                self.logger.info(new_log)
            case "WARN":
                self.logger.warn(new_log)
            case "ERROR":
                self.logger.error(new_log)
        if hasattr(self, "state"):
            self.state.run_logs.log(severity.upper(), new_log)


def init_logger(local: bool, server: bool) -> Logger:
    _addGcloudLoggingLevel()
    logger = logging.getLogger(__name__)

    if not local:
        if server:
            handler = server_cloud_handler()
        else:
            handler = job_cloud_handler()
        logger.addHandler(handler)

    return logger


def server_cloud_handler():

    region = retrieve_metadata_server(_REGION_ID)
    project = retrieve_metadata_server(_PROJECT_NAME)

    cr_job_resource = Resource(
        type="cloud_run_revision",
        labels={
            "location":  region.split("/")[-1] if region else "",
            "project_id": project,
        }
    )
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, resource=cr_job_resource)

    return handler


def job_cloud_handler():

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
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, resource=cr_job_resource, labels=labels)

    return handler


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
