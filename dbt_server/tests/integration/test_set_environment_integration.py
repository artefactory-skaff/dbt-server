import sys
import os
from google.cloud import logging

sys.path.insert(1, './lib')

from set_environment import set_env_vars, set_env_vars_job
from cloud_storage import CloudStorage, connect_client
from firestore import connect_firestore_collection


def test_set_env_vars():

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DOCKER_IMAGE'] = 'IMAGE'
    os.environ['SERVICE_ACCOUNT'] = 'ACCOUNT'
    os.environ['PROJECT_ID'] = 'ID'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'europe-west9')

    os.environ['LOCATION'] = 'LOCATION'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'LOCATION')


def test_set_env_vars_job():

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DBT_COMMAND'] = 'COMMAND'
    os.environ['UUID'] = 'UUID'

    BUCKET_NAME, DBT_COMMAND, UUID, DBT_LOGGER, STATE = set_env_vars_job(CloudStorage(connect_client()),
                                                                         connect_firestore_collection(),
                                                                         logging_client=logging.Client())
    assert (BUCKET_NAME, DBT_COMMAND, UUID) == ('BUCKET', 'COMMAND', 'UUID')

    assert DBT_LOGGER.uuid == "UUID"

    assert STATE.uuid == "UUID"
