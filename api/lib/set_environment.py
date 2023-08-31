import os
from state import State
from logger import DbtLogger


def set_env_vars():
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    DOCKER_IMAGE = os.getenv('DOCKER_IMAGE')
    SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT')
    PROJECT_ID = os.getenv('PROJECT_ID')
    LOCATION = os.getenv('LOCATION')
    return BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION


def set_env_vars_job():
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    DBT_COMMAND = os.environ.get("DBT_COMMAND")
    UUID = os.environ.get("UUID")
    ELEMENTARY = os.environ.get("ELEMENTARY")
    DBT_LOGGER = DbtLogger(local=False, server=False)
    DBT_LOGGER.uuid = UUID
    STATE = State(UUID)
    return BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY, DBT_LOGGER, STATE
