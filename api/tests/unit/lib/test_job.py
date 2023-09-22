import pytest
from unittest.mock import Mock, patch
from api.lib.job import Job, CloudRunJob, ContainerAppsJob, JobFactory
from api.lib.state import State
from api.lib.dbt_classes import DbtCommand


@patch('api.lib.job.State')
@patch('api.lib.job.DbtCommand')
def test_job_create(mock_dbt_command, mock_state):
    service = Mock()
    job = Job(service)
    job.create(mock_state, mock_dbt_command)
    service.create.assert_called_once_with(mock_state, mock_dbt_command)

@patch('api.lib.job.State')
def test_job_launch(mock_state):
    service = Mock()
    job = Job(service)
    job.launch(mock_state, 'job_name')
    service.launch.assert_called_once_with(mock_state, 'job_name')

@patch('api.lib.job.State')
@patch('api.lib.job.DbtCommand')
@patch('api.lib.job.run_v2')
def test_cloud_run_job_create(mock_run_v2, mock_dbt_command, mock_state):
    job = CloudRunJob()
    job.create(mock_state, mock_dbt_command)
    mock_run_v2.JobsClient.assert_called_once()


@patch('api.lib.job.State')
@patch('api.lib.job.run_v2')
def test_cloud_run_job_launch(mock_run_v2, mock_state):
    job = CloudRunJob()
    job.launch(mock_state, 'job_name')
    mock_run_v2.JobsClient.assert_called_once()


@patch('api.lib.job.DefaultAzureCredential')
@patch('api.lib.job.SubscriptionClient')
@patch('api.lib.job.ResourceManagementClient')
@patch('api.lib.job.settings')
@patch('api.lib.job.State')
@patch('api.lib.job.DbtCommand')
@patch('api.lib.job.ContainerInstanceManagementClient')
def test_container_apps_job_create(mock_container_client, mock_dbt_command, mock_state, settings, *args):
    settings.azure.resource_group_name.return_value = None
    job = ContainerAppsJob()
    job.create(mock_state, mock_dbt_command)
    mock_container_client.assert_called_once()

def test_job_factory_create():
    job = JobFactory.create('CloudRunJob')
    assert isinstance(job, CloudRunJob)
    job = JobFactory.create('ContainerAppsJob')
    assert isinstance(job, ContainerAppsJob)
    with pytest.raises(ValueError):
        JobFactory.create('InvalidJob')
