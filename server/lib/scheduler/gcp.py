import os

from google.api_core.exceptions import AlreadyExists
from google.cloud.scheduler_v1 import HttpTarget, HttpMethod

from server.lib.scheduler.base import BaseScheduler


class GCPScheduler(BaseScheduler):

    def __init__(self):
        from google.cloud import scheduler_v1
        self.project_id = os.environ["GCP_PROJECT_ID"]
        self.service_account_email = os.environ["GCP_SERVICE_ACCOUNT_EMAIL"]
        self.location = os.getenv("GCP_LOCATION", "europe-west1")
        self.job_parent = f"projects/{self.project_id}/locations/{self.location}"
        self.client = scheduler_v1.CloudSchedulerClient()

    def create_or_update_job(self, job_name: str, cron_expression: str, server_url: str, description: str = ""):
        job = {
            "name": f"{self.job_parent}/jobs/{job_name}",
            "schedule": cron_expression,
            "http_target": HttpTarget(
                uri=server_url,
                http_method=HttpMethod.POST,
                oidc_token={"service_account_email": self.service_account_email}
            ),
            "description": description,
            "retry_config": {
                "retry_count": 2,
                "max_retry_duration": "120s",
                "min_backoff_duration": "5s",
                "max_backoff_duration": "60s",
                "max_doublings": 5
            }
        }
        try:
            self.client.create_job(parent=self.job_parent, job=job)
        except AlreadyExists:
            print("updating job")
            self.client.delete_job(name=job["name"])
            self.client.create_job(parent=self.job_parent, job=job)
