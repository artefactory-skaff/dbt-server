
from fastapi import FastAPI, status
from google.cloud import run_v2
import os
import uuid
import uvicorn

from utils import parse_args, dbt_command
from metadata import get_project_id, get_location, get_service_account
from state import State

BUCKET_NAME = os.getenv('BUCKET_NAME')
DOCKER_IMAGE = os.getenv('DOCKER_IMAGE')
SERVICE_ACCOUNT = get_service_account()
PROJECT_ID = get_project_id()
LOCATION = get_location()

app = FastAPI()


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: dbt_command):
    print("Received command '{main_command}' and args {args}".format(
        main_command=dbt_command.command,
        args=dbt_command.args)
        )

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid)
    state.init_state()
    state.set_status("pending")

    start_cloud_run_job(dbt_command, state)
    return {"uuid": request_uuid}


def start_cloud_run_job(dbt_command: dbt_command, state: State):
    print(
        "Starting cloud run job {uuid} with command '{main_command}' and args {args}".format(
            uuid=state.get_uuid(),
            main_command=dbt_command.command,
            args=dbt_command.args)
        )

    state.set_status("running")
    state.load_context(dbt_command)

    response_job = create_job(dbt_command, state.get_uuid())
    launch_job(response_job)


def create_job(dbt_command: dbt_command, request_uuid: str):
    # Create a client
    client = run_v2.JobsClient()
    cli_args = [dbt_command.command] + parse_args(dbt_command.args)
    task_container = {
        "image": DOCKER_IMAGE,
        "env": [
            {"name": "DBT_COMMAND", "value": ' '.join(cli_args)},
            {"name": "UUID", "value": request_uuid},
            {"name": "SCRIPT", "value": "job.py"},
            {"name": "BUCKET_NAME", "value": BUCKET_NAME}
            ]
        }
    # job_id must start with a letter and cannot contain '-'
    job_id = "u"+request_uuid.replace('-', '')
    job_parent = "projects/"+PROJECT_ID+"/locations/"+LOCATION

    # Initialize request argument(s)
    job = run_v2.Job()
    job.template.template.max_retries = 1
    job.template.template.containers = [task_container]
    job.template.template.service_account = SERVICE_ACCOUNT

    request = run_v2.CreateJobRequest(
        parent=job_parent,
        job=job,
        job_id=job_id,
    )
    operation = client.create_job(request=request)

    print("Waiting for operation to complete...")

    response = operation.result()
    print(response)
    return response


def launch_job(response_job: run_v2.types.Job):
    # Create a client
    client = run_v2.JobsClient()
    job_name = response_job.name
    print("job_name:", job_name)

    # Initialize request argument(s)
    request = run_v2.RunJobRequest(
        name=job_name,
    )

    # Make the request
    client.run_job(request=request)


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=int(os.environ.get("PORT", 8001)),
        host="0.0.0.0",
        reload=True)
