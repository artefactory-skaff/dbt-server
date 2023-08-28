
from fastapi import FastAPI, status
from google.cloud import run_v2
import os
import uuid
import uvicorn
import sys

sys.path.insert(1, './lib')

from dbt_classes import DbtCommand
from command_processor import process_command
from state import State
from cloud_storage import get_blob_from_bucket
from set_environment import set_env_vars
from logger import DbtLogger


BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION = set_env_vars()

if len(sys.argv) == 2 and sys.argv[1] == "--local":  # run locally:
    LOCAL = True
    DBT_LOGGER = DbtLogger(local=True, server=True)

else:  # run on GCP:
    LOCAL = False
    DBT_LOGGER = DbtLogger(local=False, server=True)

app = FastAPI()


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand):

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid)
    state.init_state()
    state.run_status = "pending"
    DBT_LOGGER.uuid = request_uuid

    log = f"Received command '{dbt_command.user_command}'"
    DBT_LOGGER.log("INFO", log)
    state.user_command = dbt_command.user_command

    processed_command = process_command(dbt_command.user_command)
    log = f"Processed command: {processed_command}"
    DBT_LOGGER.log("INFO", log)
    dbt_command.processed_command = processed_command

    state.load_context(dbt_command)

    response_job = create_job(state, dbt_command)
    launch_job(state, response_job)

    return {"uuid": request_uuid}


def create_job(state: State, dbt_command: DbtCommand) -> run_v2.types.Job:
    log = f"Creating cloud run job {state.uuid} with command '{dbt_command.processed_command}'"
    DBT_LOGGER.log("INFO", log)

    client = run_v2.JobsClient()
    task_container = {
        "image": DOCKER_IMAGE,
        "env": [
            {"name": "DBT_COMMAND", "value": dbt_command.processed_command},
            {"name": "UUID", "value": state.uuid},
            {"name": "SCRIPT", "value": "dbt_run_job.py"},
            {"name": "BUCKET_NAME", "value": BUCKET_NAME},
            {"name": "ELEMENTARY", "value": str(dbt_command.elementary)}
            ]
        }
    # job_id must start with a letter and cannot contain '-'
    job_id = "u"+state.uuid.replace('-', '')
    job_parent = "projects/"+PROJECT_ID+"/locations/"+LOCATION

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
    DBT_LOGGER.log("INFO", log)

    response = operation.result()
    log = f"Job created: {response.name}"
    DBT_LOGGER.log("INFO", log)

    return response


def launch_job(state: State, response_job: run_v2.types.Job):

    job_name = response_job.name
    log = f"Launching job: {job_name}'"
    DBT_LOGGER.log("INFO", log)

    client = run_v2.JobsClient()
    request = run_v2.RunJobRequest(name=job_name,)

    client.run_job(request=request)
    state.run_status = "running"


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


@app.get("/report/{uuid}", status_code=status.HTTP_200_OK)
def get_report(uuid: str):
    state = State(uuid)
    cloud_storage_folder = state.storage_folder
    report = get_blob_from_bucket(BUCKET_NAME, cloud_storage_folder+'/elementary_report.html')
    _ = report

    google_console_url = 'https://console.cloud.google.com/storage/browser/_details'
    url = f'{google_console_url}/{BUCKET_NAME}/{cloud_storage_folder}/elementary_report.html'
    return {"url": url}


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=int(os.environ.get("PORT", 8001)),
        host="0.0.0.0",
        reload=True)
