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


class DbtRemoteJobs(BaseModel):
    dbt_remote_jobs: list[DbtRemoteJob]

    @computed_field
    def dbt_remote_jobs_dict(self) -> dict[str, DbtRemoteJob]:
        return {dbt_remote_job.run_id: dbt_remote_job for dbt_remote_job in self.dbt_remote_jobs}


class DbtRemoteJobManager:
    def __init__(self, server: DbtServer):
        self.server = server

    def list(self) -> DbtRemoteJobs:
        res = self.server.session.get(url=self.server.server_url + "api/run")
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
