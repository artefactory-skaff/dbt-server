from dataclasses import dataclass
from google.cloud.scheduler_v1 import HttpTarget, HttpMethod, CloudSchedulerClient
from google.api_core.exceptions import AlreadyExists, NotFound


@dataclass
class SchedulerHTTPJobSpec:
    job_name: str
    schedule: str
    target_uri: str
    description: str = ""


class CloudScheduler:
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.parent = f"projects/{self.project_id}/locations/{self.location}"
        self.client = CloudSchedulerClient()

    def create_http_scheduled_job(self, scheduler_job_spec: SchedulerHTTPJobSpec):
        job = {
            "name": f"{self.parent}/jobs/{scheduler_job_spec.job_name}",
            "schedule": scheduler_job_spec.schedule,
            "http_target": HttpTarget(uri=scheduler_job_spec.target_uri, http_method=HttpMethod.POST, oidc_token={"service_account_email": f"dbt-server-service-account@{self.project_id}.iam.gserviceaccount.com"}),
            "description": scheduler_job_spec.description,
        }

        try:
            self.client.create_job(parent=self.parent, job=job)
        except AlreadyExists:
            self.client.update_job(job=job)

    def list(self):
        jobs = self.client.list_jobs(parent=self.parent)
        return list(jobs)

    def delete(self, name: str) -> bool:
        try:
            self.client.delete_job(name=f"{self.parent}/jobs/{name}")
            return True
        except NotFound:
            return False

if __name__ == "__main__":
    schedules = CloudScheduler(project_id="dbt-server-alexis6-16ab", location="europe-west1")
    spec = SchedulerHTTPJobSpec(
        job_name="dbt-server-51ca189b-b935-4391-89a0-ee775a0681bf",
        schedule="*/10 * * * *",
        target_uri="https://dbt-server-5a442voexq-ew.a.run.app/schedule/51ca189b-b935-4391-89a0-ee775a0681bf/start",
        description="run"
    )

    schedules.create_http_scheduled_job(spec)
