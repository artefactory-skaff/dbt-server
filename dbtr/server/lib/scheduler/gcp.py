import os

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import scheduler_v1
from google.cloud.scheduler_v1 import HttpTarget, HttpMethod
from google.auth import default
from google.auth.transport.requests import Request


from dbtr.server.lib.scheduler.base import BaseScheduler


class GCPScheduler(BaseScheduler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.project_id = os.environ["PROJECT_ID"]
        self.location = os.environ["LOCATION"]
        self.parent = f"projects/{self.project_id}/locations/{self.location}"
        self.client = scheduler_v1.CloudSchedulerClient()

    def create_or_update_job(self, name: str, cron_expression: str, trigger_url: str, description: str = ""):
        job = {
            "name": f"{self.parent}/jobs/{name}",
            "schedule": cron_expression,
            "http_target": HttpTarget(
                uri=trigger_url,
                http_method=HttpMethod.POST,
                oidc_token={"service_account_email": get_service_account_email()}
            ),
            "description": description,
            "retry_config": {
                "retry_count": 0,
            }
        }
        try:
            self.client.create_job(parent=self.parent, job=job)
        except AlreadyExists:
            self.client.delete_job(name=job["name"])
            self.client.create_job(parent=self.parent, job=job)

    def delete(self, name: str) -> bool:
        try:
            self.client.delete_job(name=f"{self.parent}/jobs/{name}")
            return True
        except NotFound:
            return False

    def list(self):
        jobs = self.client.list_jobs(parent=self.parent)
        return list(jobs)


def get_service_account_email(scopes=["https://www.googleapis.com/auth/cloud-platform"]):
    credentials, _ = default(scopes=scopes)
    credentials.refresh(Request())
    return credentials.service_account_email
