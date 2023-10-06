from unittest.mock import Mock, patch

import pytest
from dbt_server.lib.dbt_classes import DbtCommand
from dbt_server.lib.job import CloudRunJob, ContainerAppsJob, Job, JobFactory
from dbt_server.lib.state import State


@patch("dbt_server.lib.job.State")
@patch("dbt_server.lib.job.DbtCommand")
@patch("dbt_server.lib.job.run_v2")
@patch("dbt_server.lib.job.settings")
def test_cloud_run_job_create(mock_settings, mock_run_v2, mock_dbt_command, mock_state):
    mock_settings.gcp.project_id.return_value = "test"
    job = CloudRunJob()
    job.create(mock_state, mock_dbt_command)
    mock_run_v2.JobsClient.assert_called_once()


@patch("dbt_server.lib.job.State")
@patch("dbt_server.lib.job.run_v2")
def test_cloud_run_job_launch(mock_run_v2, mock_state):
    job = CloudRunJob()
    job.launch(mock_state, "job_name")
    mock_run_v2.JobsClient.assert_called_once()


@patch("dbt_server.lib.job.DefaultAzureCredential")
@patch("dbt_server.lib.job.SubscriptionClient")
@patch("dbt_server.lib.job.ResourceManagementClient")
@patch("dbt_server.lib.job.settings")
@patch("dbt_server.lib.job.State")
@patch("dbt_server.lib.job.DbtCommand")
@patch("dbt_server.lib.job.ContainerInstanceManagementClient")
def test_container_apps_job_create(
    mock_container_client, mock_dbt_command, mock_state, settings, *args
):
    settings.azure.resource_group_name.return_value = None
    job = ContainerAppsJob()
    job.create(mock_state, mock_dbt_command)
    mock_container_client.assert_called_once()


def test_job_factory_create():
    job = JobFactory.create("CloudRunJob")
    assert isinstance(job, CloudRunJob)
    job = JobFactory.create("ContainerAppsJob")
    assert isinstance(job, ContainerAppsJob)
    with pytest.raises(ValueError):
        JobFactory.create("InvalidJob")
