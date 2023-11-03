from dataclasses import dataclass
from google.cloud import run_v2

from dbt_server.lib.state import State
from dbt_server.lib.logger import DbtLogger


@dataclass
class DbtCloudRunJobConfig:
    uuid: str
    dbt_command: str
    project_id: str
    location: str
    service_account: str
    job_docker_image: str
    artifacts_bucket_name: str


class DbtCloudRunJobStarter:
    def __init__(self, dbt_job_config: DbtCloudRunJobConfig, logger: DbtLogger):
        self.dbt_job_config = dbt_job_config
        self.state = State.from_uuid(dbt_job_config.uuid)
        self.logger = logger

    def start(self) -> None:
        job = self.create_job()
        self.launch_job(job)

    def create_job(self) -> run_v2.types.Job:
        self.logger.log("INFO", f"Creating cloud run job {self.state.uuid} with command 'dbt {self.dbt_job_config.dbt_command}'")

        job = run_v2.Job()
        job.template.template.max_retries = 0
        job.template.template.service_account = self.dbt_job_config.service_account
        job.template.template.containers = [{
            "image": self.dbt_job_config.job_docker_image,
            "env": [
                {"name": "DBT_COMMAND", "value": self.dbt_job_config.dbt_command},
                {"name": "UUID", "value": self.state.uuid},
                {"name": "SCRIPT", "value": "dbt_server/dbt_run_job.py"},
                {"name": "BUCKET_NAME", "value": self.dbt_job_config.artifacts_bucket_name},
            ]
        }]

        request = run_v2.CreateJobRequest(
            parent=f"projects/{self.dbt_job_config.project_id}/locations/{self.dbt_job_config.location}",
            job_id=f"u{self.state.uuid.replace('-', '')}", # job_id must start with a letter and cannot contain '-'
            job=job
        )

        try:
            operation = run_v2.JobsClient().create_job(request=request)
        except Exception:
            raise DbtCloudRunJobCreationFailed(f"Cloud Run job creation failed")

        response = operation.result()
        self.logger.log("INFO", f"Job created: {response.name}")

        return response


    def launch_job(self, job: run_v2.types.Job):
        self.logger.log("INFO", f"Starting job: {job.name}'")

        client = run_v2.JobsClient()
        request = run_v2.RunJobRequest(name=job.name)

        try:
            client.run_job(request=request)
        except Exception:
            raise DbtCloudRunJobStartFailed(f"Cloud Run job start failed")

        self.state.run_status = "running"


class DbtCloudRunJobCreationFailed(Exception):
    pass

class DbtCloudRunJobStartFailed(Exception):
    pass
