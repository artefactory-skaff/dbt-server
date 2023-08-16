
from fastapi import FastAPI, status
from google.cloud import run_v2
import google.cloud.logging
from google.cloud.logging import DESCENDING
import logging
import os
import uuid
import uvicorn
import sys

from dbt_types import dbt_command
from utils import process_command
from metadata import get_project_id, get_location, get_service_account
from state import State


BUCKET_NAME = os.getenv('BUCKET_NAME')
DOCKER_IMAGE = os.getenv('DOCKER_IMAGE')

# run locally:
if len(sys.argv) == 2 and sys.argv[1] == "--local":
    LOCAL = True

    from dotenv import load_dotenv
    from pathlib import Path

    dotenv_path = Path('.env.local_server')
    load_dotenv(dotenv_path=dotenv_path)

    SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT')
    PROJECT_ID = os.getenv('PROJECT_ID')
    LOCATION = os.getenv('LOCATION')

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# run on GCP:
else:
    LOCAL = False

    SERVICE_ACCOUNT = get_service_account()
    PROJECT_ID = get_project_id()
    LOCATION = get_location()

    client = google.cloud.logging.Client()
    client.setup_logging()

app = FastAPI()


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_state(uuid: str):
    job_state = State(uuid)
    return {"run_status": job_state.run_status, "entries": job_state.run_logs}


@app.get("/errors/{timestamp}", status_code=status.HTTP_200_OK)
def get_server_errors(timestamp: str):
    if LOCAL:
        return {"logs": ["Server running in local"]}

    filter_str = (
        '(resource.type="cloud_run_revision")'
        ' AND severity = ERROR'
        f' AND timestamp>="{timestamp}"'
    )

    entries = client.list_entries(filter_=filter_str, order_by=DESCENDING, max_results=5)
    logs = []
    for log in entries:
        logs.append(str(log.payload))
    return {"logs": logs}


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: dbt_command):
    logging.info("Received command '{command}'".format(
        command=dbt_command.command)
        )

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid)
    state.init_state()
    state.run_status = "pending"
    state.run_logs = "INFO\t Received command '{command}'".format(
        command=dbt_command.command)

    start_cloud_run_job(dbt_command, state)
    return {"uuid": request_uuid}


def start_cloud_run_job(dbt_command: dbt_command, state: State):
    logging.info(
        "Starting cloud run job {uuid} with command '{main_command}'".format(
            uuid=state.uuid,
            main_command=dbt_command.command)
        )
    state.run_logs = "INFO\t Starting cloud run job {uuid} with command '{main_command}'".format(
        uuid=state.uuid,
        main_command=dbt_command.command)

    state.run_status = "running"
    state.load_context(dbt_command)

    processed_command = process_command(state, dbt_command.command)
    logging.info('processed command: '+processed_command)
    state.run_logs = 'INFO\t Processed command: '+processed_command

    response_job = create_job(processed_command, state)
    launch_job(response_job, state)


def create_job(command: str, state: State):
    # Create a client
    client = run_v2.JobsClient()
    task_container = {
        "image": DOCKER_IMAGE,
        "env": [
            {"name": "DBT_COMMAND", "value": command},
            {"name": "UUID", "value": state.uuid},
            {"name": "SCRIPT", "value": "job.py"},
            {"name": "BUCKET_NAME", "value": BUCKET_NAME}
            ]
        }
    # job_id must start with a letter and cannot contain '-'
    job_id = "u"+state.uuid.replace('-', '')
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

    logging.info("Waiting for job creation to complete...")
    state.run_logs = "INFO\t Waiting for job creation to complete..."

    response = operation.result()
    logging.info({"response": str(response)})
    state.run_logs = "INFO\t Job created: " + response.name
    return response


def launch_job(response_job: run_v2.types.Job, state: State):
    # Create a client
    client = run_v2.JobsClient()
    job_name = response_job.name
    logging.info("job_name:{job}".format(job=job_name))
    state.run_logs = "INFO\t Launching job: "+job_name

    # Initialize request argument(s)
    request = run_v2.RunJobRequest(name=job_name,)

    # Make the request
    client.run_job(request=request)


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=int(os.environ.get("PORT", 8001)),
        host="0.0.0.0",
        reload=True)
