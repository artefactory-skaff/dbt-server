import sys
import os
from unittest.mock import Mock

sys.path.insert(1, './api/lib')

from set_environment import set_env_vars, set_env_vars_job


def test_set_env_vars():

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DOCKER_IMAGE'] = 'IMAGE'
    os.environ['SERVICE_ACCOUNT'] = 'ACCOUNT'
    os.environ['PROJECT_ID'] = 'ID'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'us-central1')

    os.environ['LOCATION'] = 'LOCATION'

    assert set_env_vars() == ('BUCKET', 'IMAGE', 'ACCOUNT', 'ID', 'LOCATION')


def test_set_env_vars_job(MockCloudStorage, MockState):
    mock_gcs_client, _, _, _ = MockCloudStorage
    mock_dbt_collection, _ = MockState
    uuid = "UUID"
    mock_logging_client = Mock(name='mock_logging_client')

    os.environ['BUCKET_NAME'] = 'BUCKET'
    os.environ['DBT_COMMAND'] = 'COMMAND'
    os.environ['UUID'] = uuid
    os.environ['ELEMENTARY'] = 'True'

    BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY, DBT_LOGGER, STATE = set_env_vars_job(mock_gcs_client,
                                                                                     mock_dbt_collection,
                                                                                     mock_logging_client)
    assert (BUCKET_NAME, DBT_COMMAND, UUID, ELEMENTARY) == ('BUCKET', 'COMMAND', 'UUID', 'True')
    assert DBT_LOGGER.uuid == uuid
    assert STATE.uuid == uuid
