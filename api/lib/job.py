from typing import Dict, Any
import traceback
import subprocess

from fastapi import HTTPException

try:
    from google.cloud import run_v2
except ImportError:
    run_v2 = None
try:
    from azure.containerinstance import ContainerInstanceManagementClient
    from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
    from azure.identity import DefaultAzureCredential
except ImportError:
    ContainerInstanceManagementClient = None
    ResourceManagementClient, SubscriptionClient = None, None
    DefaultAzureCredential = None


from api.config import Settings
from api.clients import LOGGER
from api.lib.state import State
from api.lib.dbt_classes import DbtCommand


settings = Settings()


class Job:
    def __init__(self, service):
        self.service = service

    def create(self, state: State, dbt_command: DbtCommand) -> str:
        return self.service.create(state, dbt_command)

    def launch(self, state: State, job_name: str) -> None:
        self.service.launch(state, job_name)


class LocalJob:
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        task_container = {
            "image": settings.docker_image,
            "env": [
                {"name": "DBT_COMMAND", "value": dbt_command.processed_command},
                {"name": "UUID", "value": state.uuid},
                {"name": "SCRIPT", "value": "dbt_run_job.py"},
                {"name": "BUCKET_NAME", "value": settings.bucket_name},
                {"name": "ELEMENTARY", "value": str(dbt_command.elementary)},
            ],
        }
        env_vars = " ".join(
            [f'-e {var["name"]}={var["value"]}' for var in task_container["env"]]
        )
        command = f'docker run --rm -d {env_vars} {task_container["image"]}'
        subprocess.Popen(command, shell=True)

    def launch(self, state: State, job_name: str) -> None:
        state.run_status = "running"


class CloudRunJob:
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        LOGGER.log(
            "INFO",
            f"Creating cloud run job {state.uuid} with command '{dbt_command.processed_command}'",
        )
        client = run_v2.JobsClient()
        task_container = {
            "image": settings.docker_image,
            "env": [
                {"name": "DBT_COMMAND", "value": dbt_command.processed_command},
                {"name": "UUID", "value": state.uuid},
                {"name": "SCRIPT", "value": "dbt_run_job.py"},
                {"name": "BUCKET_NAME", "value": settings.bucket_name},
                {"name": "ELEMENTARY", "value": str(dbt_command.elementary)},
            ],
        }
        # job_id must start with a letter and cannot contain '-'
        job_id = "u" + state.uuid.replace("-", "")
        job_parent = (
            "projects/"
            + settings.gcp.project_id
            + "/locations/"
            + settings.gcp.location
        )
        job = run_v2.Job()
        job.template.template.max_retries = 0
        job.template.template.containers = [task_container]
        job.template.template.service_account = settings.gcp.service_account
        request = run_v2.CreateJobRequest(
            parent=job_parent,
            job=job,
            job_id=job_id,
        )
        try:
            operation = client.create_job(request=request)
        except Exception:
            traceback_str = traceback.format_exc()
            raise HTTPException(
                status_code=400, detail="Cloud Run job creation failed" + traceback_str
            )
        LOGGER.log("INFO", "Waiting for job creation to complete...")
        response = operation.result()
        LOGGER.log("INFO", f"Job created: {response.name}")
        return response.name

    def launch(self, state: State, job_name: str) -> None:
        LOGGER.log("INFO", f"Launching job: {job_name}'")
        client = run_v2.JobsClient()
        request = run_v2.RunJobRequest(
            name=job_name,
        )
        try:
            client.run_job(request=request)
        except Exception:
            traceback_str = traceback.format_exc()
            raise HTTPException(
                status_code=400, detail="Cloud Run job start failed" + traceback_str
            )
        state.run_status = "running"


class ContainerAppsJob:
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        LOGGER.log(
            "INFO",
            f"Creating Azure Container job {state.uuid} with command '{dbt_command.processed_command}'",
        )
        credential = DefaultAzureCredential()
        subscription_client = SubscriptionClient(credential)
        subscription_id = next(subscription_client.subscriptions.list())
        resource_client = ResourceManagementClient(credential, subscription_id)
        container_client = ContainerInstanceManagementClient(
            credential, subscription_id
        )

        # job_id must start with a letter and cannot contain '-'
        job_id = "u" + state.uuid.replace("-", "")
        resource_group_name = settings.azure.resource_group_name

        task_container = {
            "name": job_id,
            "image": settings.docker_image,
            "environment_variables": [
                {"name": "DBT_COMMAND", "value": dbt_command.processed_command},
                {"name": "UUID", "value": state.uuid},
                {"name": "SCRIPT", "value": "dbt_run_job.py"},
                {"name": "BUCKET_NAME", "value": settings.bucket_name},
                {"name": "ELEMENTARY", "value": str(dbt_command.elementary)},
            ],
            "resources": {"requests": {"cpu": 1.0, "memory_in_gb": 1.5}},
        }

        container_group = container_client.container_groups.begin_create_or_update(
            resource_group_name,
            job_id,
            {
                "location": settings.azure.location,
                "containers": [task_container],
                "os_type": "Linux",
                "restart_policy": "Never",
            },
        )

        LOGGER.log("INFO", f"Job created: {container_group.result().name}")
        return container_group.result().name

    def launch(self, state: State, job_name: str) -> None:
        # In Azure Container Instances, the job starts running as soon as it's created.
        # So there's no need for a separate launch method.
        state.run_status = "running"


class JobFactory:
    @staticmethod
    def create(service_type):
        if service_type == "CloudRunJob":
            return CloudRunJob()
        elif service_type == "ContainerAppsJob":
            return ContainerAppsJob()
        elif service_type == "LocalJob":
            return LocalJob()
        else:
            raise ValueError("Invalid service type")
