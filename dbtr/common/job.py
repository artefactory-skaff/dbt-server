from enum import Enum
from typing import Dict, Optional

from pydantic import Field, BaseModel, computed_field

from dbtr.common.remote_server import DbtServer


class DbtRemoteJob(BaseModel):
    run_id: Optional[str] = Field(default=None, validate_default=True)
    run_conf_version: int = 1
    project: str
    server_url: str
    cloud_provider: str
    provider_config: Dict = Field(default_factory=dict)
    requester: str = "unknown"
    dbt_runtime_config: Dict = Field(default_factory=dict)
    schedule_name: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_description: Optional[str] = None

    @computed_field
    def humanized_model_selection(self) -> str:
        selected_models = self.dbt_runtime_config["flags"].get("select", [])
        excluded_models = self.dbt_runtime_config["flags"].get("exclude", [])

        if len(excluded_models) > 0:
            if len(selected_models) > 0:
                selected_models_string = ', '.join(selected_models)
                excluded_models_string = f"excluding {', '.join(excluded_models)}"
            else:
                selected_models_string = ""
                excluded_models_string = f"All models excluding {', '.join(excluded_models)}"
        else:
            if len(selected_models) > 0:
                selected_models_string = f"{', '.join(selected_models)}"
                excluded_models_string = ""
            else:
                selected_models_string = "All models"
                excluded_models_string = ""

        model_selection_string = f"{selected_models_string} {excluded_models_string}"
        return model_selection_string


class DbtRemoteJobs(BaseModel):
    dbt_remote_jobs: list[DbtRemoteJob]

    @computed_field
    def dbt_remote_jobs_dict(self) -> dict[str, DbtRemoteJob]:
        return {dbt_remote_job.run_id: dbt_remote_job for dbt_remote_job in self.dbt_remote_jobs}


class DbtRemoteJobManager:
    def __init__(self, server: DbtServer):
        self.server = server

    def list(self, skip: int = 0, limit: int = 20) -> DbtRemoteJobs:
        res = self.server.session.get(url=self.server.server_url + f"api/run?skip={skip}&limit={limit}")
        return DbtRemoteJobs(dbt_remote_jobs=[DbtRemoteJob(**dbt_remote_job_dict) for dbt_remote_job_dict in res.json().values()])

    def get(self, dbt_remote_job_id: str) -> DbtRemoteJob:
        res = self.server.session.get(url=self.server.server_url + f"api/run/{dbt_remote_job_id}")
        dbt_remote_job_dict = res.json()
        return DbtRemoteJob(**dbt_remote_job_dict)


class JobStatus(str, Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SERVER_ERROR = "server error"
