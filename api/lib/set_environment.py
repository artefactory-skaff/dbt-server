import os
from typing import List
from google.cloud.logging import Client

from lib.state import State
from lib.logger import DbtLogger
from lib.cloud_storage import CloudStorage
from google.cloud import firestore


def set_env_vars() -> tuple[str | None, str | None, str | None, str | None, str | None]:
    BUCKET_NAME = os.getenv('BUCKET_NAME', default='dbt-stc-test')
    DOCKER_IMAGE = os.getenv('DOCKER_IMAGE',
                             default='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod')
    SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT', default='stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com')
    PROJECT_ID = os.getenv('PROJECT_ID', default='stc-dbt-test-9e19')
    LOCATION = os.getenv('LOCATION', default='us-central1')
    return BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION


def set_env_vars_job(cloud_storage_instance: CloudStorage,
                     dbt_collection: firestore.CollectionReference,
                     logging_client: Client) -> tuple[str | None, str | None, str | None,
                                                      str | None, DbtLogger, State]:
    BUCKET_NAME = os.getenv('BUCKET_NAME', default='dbt-stc-test')
    DBT_COMMAND = os.environ.get("DBT_COMMAND", default='')
    UUID = os.environ.get("UUID", default='0000')
    ELEMENTARY = os.environ.get("ELEMENTARY", default='False')
    DBT_LOGGER = DbtLogger(cloud_storage_instance=cloud_storage_instance, dbt_collection=dbt_collection,
                           logging_client=logging_client, local=False, server=False)
    DBT_LOGGER.uuid = UUID
    STATE = State(UUID, cloud_storage_instance, dbt_collection)
    return BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY, DBT_LOGGER, STATE


def get_server_dbt_logger(client_storage_instance: CloudStorage, dbt_collection: firestore.CollectionReference,
                          logging_client: Client, argv: List[str]) -> DbtLogger:
    local = False
    if len(argv) == 2 and argv[1] == "--local":  # run locally:
        local = True
    DBT_LOGGER = DbtLogger(cloud_storage_instance=client_storage_instance, dbt_collection=dbt_collection,
                           logging_client=logging_client, local=local, server=True)
    return DBT_LOGGER
