import sys
import os

sys.path.insert(1, './api/lib')

from set_environment import set_env_vars, set_env_vars_job
from logger import DbtLogger
from state import State


def test_set_env_vars():

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DOCKER_IMAGE'] = 'IMAGE'
    os.environ['SERVICE_ACCOUNT'] = 'ACCOUNT'
    os.environ['PROJECT_ID'] = 'ID'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'us-central1')

    os.environ['LOCATION'] = 'LOCATION'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'LOCATION')


def test_set_env_vars_job():

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DBT_COMMAND'] = 'COMMAND'
    os.environ['UUID'] = 'UUID'
    os.environ['ELEMENTARY'] = 'True'
    DBT_LOGGER2 = DbtLogger(local=False, server=False)
    DBT_LOGGER2.uuid = 'UUID'
    STATE2 = State('UUID')

    BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY, DBT_LOGGER, STATE = set_env_vars_job()
    assert (BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY) == ('BUCKET', 'COMMAND', 'UUID', 'True')

    assert DBT_LOGGER2.uuid == DBT_LOGGER.uuid

    assert STATE.uuid == STATE2.uuid
