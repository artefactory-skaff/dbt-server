from typing import Any, Dict, List

import subprocess
import traceback
from abc import ABC, abstractmethod
from enum import Enum

from fastapi import HTTPException

try:
    from google.cloud import run_v2
except ImportError:
    run_v2 = None  # type: ignore
try:
    from azure.containerinstance import ContainerInstanceManagementClient
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.resource import SubscriptionClient
except ImportError:
    ContainerInstanceManagementClient = None  # type: ignore
    ResourceManagementClient, SubscriptionClient = None, None  # type: ignore
    DefaultAzureCredential = None  # type: ignore


from dbt_server.config import Settings
from dbt_server.lib.dbt_classes import DbtCommand
from dbt_server.lib.logger import LOGGER
from dbt_server.lib.state import State

settings = Settings()


def settings_to_env_vars(settings: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    # Serialize the Settings object to a dictionary
    env_vars: Dict[str, Any] = {}
    for key, value in settings.items():
        if isinstance(value, Enum):
            value = str(value)
        elif isinstance(value, dict):
            # If the value is a dictionary, we need to handle nested environment variables
            env_vars = env_vars | settings_to_env_vars(value, f"{key}__")
        elif value is None:
            continue
        else:
            env_vars[f"{prefix.upper()}{key.upper()}"] = value
    return env_vars


class Job(ABC):
    @abstractmethod
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        pass

    @abstractmethod
    def launch(self, state: State, job_name: str) -> None:
        pass


class LocalJob(Job):
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        old_env_vars = settings_to_env_vars(settings.dict())
        new_env_vars = {
            "DBT_COMMAND": dbt_command.processed_command,
            "UUID": state.uuid,
            "SCRIPT": "dbt-server job run",
            "ELEMENTARY": str(dbt_command.elementary),
        }
        env_vars = old_env_vars | new_env_vars
        task_container = {
            "image": settings.docker_image,
            "env": [{"name": key, "value": value} for key, value in env_vars.items()],
        }
        formatted_env_vars = ""
        if isinstance(task_container["env"], list):
            formatted_env_vars = (
                " ".join([f'-e {var["name"]}={var["value"]}' for var in task_container["env"]])
                + " "
            )
        command = f'docker run --rm -d {formatted_env_vars}{task_container["image"]}'
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        output, error = process.communicate()
        if process.returncode != 0:
            raise Exception(f"Failed to run command. Error: {str(error)}")
        return output.decode("utf-8").strip()

    def launch(self, state: State, job_name: str) -> None:
        state.run_status = "running"


class CloudRunJob(Job):
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        LOGGER.log(
            "INFO",
            f"Creating cloud run job {state.uuid} with command '{dbt_command.processed_command}'",
        )
        old_env_vars = settings_to_env_vars(settings.dict())
        new_env_vars = {
            "DBT_COMMAND": dbt_command.processed_command,
            "UUID": state.uuid,
            "SCRIPT": "dbt-server job run",
            "ELEMENTARY": str(dbt_command.elementary),
        }
        env_vars = old_env_vars | new_env_vars
        client = run_v2.JobsClient()
        task_container = {
            "image": settings.docker_image,
            "env": [{"name": key, "value": value} for key, value in env_vars.items()],
        }
        # job_id must start with a letter and cannot contain '-'
        job_id = "u" + state.uuid.replace("-", "")
        job_parent = None
        gcp_service_account = None
        if settings.gcp:
            job_parent = (
                "projects/" + settings.gcp.project_id + "/locations/" + settings.gcp.location
            )
            gcp_service_account = settings.gcp.service_account
        job = run_v2.Job()
        job.template.template.max_retries = 0
        job.template.template.containers = [task_container]
        job.template.template.service_account = gcp_service_account
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


class ContainerAppsJob(Job):
    def create(self, state: State, dbt_command: DbtCommand) -> str:
        LOGGER.log(
            "INFO",
            f"Creating Azure Container job {state.uuid} with command '{dbt_command.processed_command}'",
        )
        old_env_vars = settings_to_env_vars(settings.dict())
        new_env_vars = {
            "DBT_COMMAND": dbt_command.processed_command,
            "UUID": state.uuid,
            "SCRIPT": "dbt-server job run",
            "ELEMENTARY": str(dbt_command.elementary),
        }
        env_vars = old_env_vars | new_env_vars
        credential = DefaultAzureCredential()
        subscription_client = SubscriptionClient(credential)
        subscription_id = next(subscription_client.subscriptions.list())
        container_client = ContainerInstanceManagementClient(credential, subscription_id)

        # job_id must start with a letter and cannot contain '-'
        job_id = "u" + state.uuid.replace("-", "")
        azure_resource_group_name = None
        azure_job_memory_in_gb = None
        azure_job_cpu = None
        azure_location = None
        if settings.azure:
            azure_resource_group_name = settings.azure.resource_group_name
            azure_job_memory_in_gb = settings.azure.job_memory_in_gb
            azure_job_cpu = settings.azure.job_cpu
            azure_location = settings.azure.location

        task_container = {
            "name": job_id,
            "image": settings.docker_image,
            "environment_variables": [
                {"name": key, "value": value} for key, value in env_vars.items()
            ],
            "resources": {
                "requests": {
                    "cpu": azure_job_cpu,
                    "memory_in_gb": azure_job_memory_in_gb,
                }
            },
        }

        container_group = container_client.container_groups.begin_create_or_update(
            azure_resource_group_name,
            job_id,
            {
                "location": azure_location,
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
