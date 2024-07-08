import datetime
from typing import Optional
import humanize

from pydantic import BaseModel, computed_field

from dbtr.common.job import DbtRemoteJob, JobStatus
from dbtr.common.remote_server import DbtServer


class JobWithStatus(DbtRemoteJob):
    run_status: JobStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @computed_field
    def start_datetime(self) -> Optional[datetime.datetime]:
        if self.start_time:
            return datetime.datetime.fromtimestamp(self.start_time)
        return None

    @computed_field
    def end_datetime(self) -> Optional[datetime.datetime]:
        if self.end_time:
            return datetime.datetime.fromtimestamp(self.end_time)
        return None

    @computed_field
    def end_time_humanized(self) -> datetime.datetime:
        return humanize.naturaltime(self.end_datetime)

    @computed_field
    def duration(self) -> Optional[datetime.timedelta]:
        if self.start_datetime and self.end_datetime:
            return self.end_datetime - self.start_datetime
        return None

    @computed_field
    def duration_humanized(self) -> Optional[str]:
        if self.duration:
            return humanize.naturaldelta(self.duration)
        return None


class JobsWithStatus(BaseModel):
    dbt_remote_jobs: list[JobWithStatus]

    @computed_field
    def dbt_remote_jobs_dict(self) -> dict[str, DbtRemoteJob]:
        return {dbt_remote_job.run_id: dbt_remote_job for dbt_remote_job in self.dbt_remote_jobs}


class JobWithStatusManager:
    def __init__(self, server: DbtServer):
        self.server = server

    def list(self, skip: int = 0, limit: int = 20, project: str = None) -> JobsWithStatus:
        project_param = f"&project={project}" if project else ""
        res = self.server.session.get(url=self.server.server_url + f"api/run?skip={skip}&limit={limit}{project_param}")
        return JobsWithStatus(dbt_remote_jobs=[JobWithStatus(**dbt_remote_job_dict) for dbt_remote_job_dict in res.json().values()])

    def get(self, dbt_remote_job_id: str) -> JobWithStatus:
        res = self.server.session.get(url=self.server.server_url + f"api/run/{dbt_remote_job_id}")
        dbt_remote_job_dict = res.json()
        return JobWithStatus(**dbt_remote_job_dict)
