import os
from state import State
from logger import DbtLogger
from typing import List


def set_env_vars() -> tuple[str | None, str | None, str | None, str | None, str | None]:
    BUCKET_NAME = os.getenv('BUCKET_NAME', default='dbt-stc-test')
    DOCKER_IMAGE = os.getenv('DOCKER_IMAGE',
                             default='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod')
    SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT', default='stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com')
    PROJECT_ID = os.getenv('PROJECT_ID', default='stc-dbt-test-9e19')
    LOCATION = os.getenv('LOCATION', default='us-central1')
    return BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION


def set_env_vars_job() -> tuple[str | None, str | None,
                                str | None, str | None, DbtLogger, State]:
    BUCKET_NAME = os.getenv('BUCKET_NAME', default='dbt-stc-test')
    DBT_COMMAND = os.environ.get("DBT_COMMAND", default='')
    UUID = os.environ.get("UUID", default='0000')
    ELEMENTARY = os.environ.get("ELEMENTARY", default='False')
    DBT_LOGGER = DbtLogger(local=False, server=False)
    DBT_LOGGER.uuid = UUID
    STATE = State(UUID)
    return BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY, DBT_LOGGER, STATE


def get_server_dbt_logger(argv: List[str]) -> DbtLogger:
    local = False
    if len(argv) == 2 and argv[1] == "--local":  # run locally:
        local = True
    DBT_LOGGER = DbtLogger(local=local, server=True)
    return DBT_LOGGER
