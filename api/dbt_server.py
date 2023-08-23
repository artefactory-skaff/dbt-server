
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
from command_processor import process_command
from metadata import get_project_id, get_location, get_service_account
from state import State
from cloud_storage import get_document_from_bucket


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
    run_status = job_state.run_status
    return {"run_status": run_status}


@app.get("/job/{uuid}/last_logs", status_code=status.HTTP_200_OK)
def get_last_logs(uuid: str):
    job_state = State(uuid)
    logs = job_state.get_last_logs()
    return {"run_logs": logs}


@app.get("/errors/{timestamp}", status_code=status.HTTP_200_OK)
def get_server_errors(timestamp: str):
    if LOCAL:
        return {"logs": ["Logs are not available on Cloud Logging: Server running in local"]}

    server_name = 'server-prod'
    filter_str = (
        '(resource.type="cloud_run_revision")'
        f'AND resource.labels.service_name = "{server_name}"'
        ' AND severity = ERROR'
        f' AND timestamp>="{timestamp}"'
    )

    entries = client.list_entries(filter_=filter_str, order_by=DESCENDING, max_results=5)
    logs = []
    for log in entries:
        logs.append(str(log.payload))
    return {"logs": logs}


@app.get("/report/{uuid}", status_code=status.HTTP_200_OK)
def get_report(uuid: str):
    state = State(uuid)
    cloud_storage_folder = state.storage_folder
    report = get_document_from_bucket(BUCKET_NAME, cloud_storage_folder+'/elementary_report.html')
    _ = report

    google_console_url = 'https://console.cloud.google.com/storage/browser/_details'
    url = f'{google_console_url}/{BUCKET_NAME}/{cloud_storage_folder}/elementary_report.html'
    return {"url": url}


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: dbt_command):

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid)
    state.init_state()
    state.run_status = "pending"

    log = f"Received command '{dbt_command.command}'"
    Log_info(state, log)
    state.user_command = dbt_command.command

    processed_command = process_command(state, dbt_command.command)
    log = f"Processed command: {processed_command}"
    Log_info(state, log)
    dbt_command.command = processed_command

    state.load_context(dbt_command)

    response_job = create_job(state, dbt_command)
    launch_job(response_job, state)

    return {"uuid": request_uuid}


def create_job(state: State, dbt_command: dbt_command) -> run_v2.types.Job:
    log = f"Creating cloud run job {state.uuid} with command '{dbt_command.command}'"
    Log_info(state, log)

    # Create a client
    client = run_v2.JobsClient()
    logging.info(str(dbt_command.elementary))
    task_container = {
        "image": DOCKER_IMAGE,
        "env": [
            {"name": "DBT_COMMAND", "value": dbt_command.command},
            {"name": "UUID", "value": state.uuid},
            {"name": "SCRIPT", "value": "job.py"},
            {"name": "BUCKET_NAME", "value": BUCKET_NAME},
            {"name": "ELEMENTARY", "value": str(dbt_command.elementary)}
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

    log = "Waiting for job creation to complete..."
    Log_info(state, log)

    response = operation.result()
    log = f"Job created: {response.name}"
    Log_info(state, log)

    return response


def launch_job(response_job: run_v2.types.Job, state: State):

    job_name = response_job.name
    log = f"Launching job: {job_name}'"
    Log_info(state, log)

    # Create a client
    client = run_v2.JobsClient()

    # Initialize request argument(s)
    request = run_v2.RunJobRequest(name=job_name,)

    # Make the request
    client.run_job(request=request)
    state.run_status = "running"


def Log_info(state: State, log: str):
    logging.info(log)
    # state.run_logs = "INFO\t"+log
    state.run_logs.info(log)


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=int(os.environ.get("PORT", 8001)),
        host="0.0.0.0",
        reload=True)
