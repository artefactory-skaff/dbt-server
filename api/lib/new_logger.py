import logging
# https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility/35804945#35804945
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.resource import Resource
from google.cloud.logging_v2.handlers._monitored_resources import retrieve_metadata_server, _REGION_ID, _PROJECT_NAME
import os


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


"""
    google logging levels:

    DEFAULT
    DEBUG
    INFO
    NOTICE
    WARNING
    ERROR
    CRITICAL
    ALERT
    EMERGENCY
"""


def _addGcloudLoggingLevel():
    _addLoggingLevel('DEFAULT', 1)
    _addLoggingLevel('NOTICE', 15)
    _addLoggingLevel('ALERT', 60)
    _addLoggingLevel('EMERGENCY', 100)

    format = '{"severity": "%(levelname)s", "message": "%(message)s","sourceLocation":{"file":"%(filename)s","line":%(lineno)d,"function":"%(funcName)s"}}'
    logging.basicConfig(level=logging.DEFAULT, format=format)  # type: ignore


def init_logger():
    _addGcloudLoggingLevel()

    logger = logging.getLogger(__name__)

    # find metadata about the execution environment
    region = retrieve_metadata_server(_REGION_ID)
    project = retrieve_metadata_server(_PROJECT_NAME)
    uuid = os.environ.get("UUID")

    # build a manual resource object
    cr_job_resource = Resource(
        type="cloud_run_job",
        labels={
            "job_name": os.environ.get('CLOUD_RUN_JOB', 'unknownJobId'),
            "location":  region.split("/")[-1] if region else "",
            "project_id": project,
            "uuid": uuid,
        }
    )
    labels = {"uuid": uuid}
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, resource=cr_job_resource, labels=labels)
    logger.addHandler(handler)
    return logger
