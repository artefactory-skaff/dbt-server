import base64
from google.cloud import run_v2
from google.cloud import logging
import os
import uuid
import uvicorn
import sys
import traceback
from pathlib import Path
from fastapi import FastAPI, status, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse

from lib.dbt_classes import DbtCommand, FollowUpLink
from lib.command_processor import process_command
from lib.state import State
from lib.cloud_storage import CloudStorage, connect_client
from lib.set_environment import set_env_vars, get_server_dbt_logger
from lib.firestore import connect_firestore_collection


BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION = set_env_vars()
PORT = os.environ.get("PORT", "8001")
DBT_LOGGER = get_server_dbt_logger(CloudStorage(connect_client()), connect_firestore_collection(),
                                   logging.Client(), sys.argv)
BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version="0.0.1",
    docs_url="/docs"
)
templates = Jinja2Templates(directory=str(Path(BASE_DIR, 'templates')))


@app.get("/")
def get_home(request: Request):
    return templates.TemplateResponse('home.html', context={'request': request})


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand):

    dbt_command = base64_decode_dbt_command(dbt_command)

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid, CloudStorage(connect_client()), connect_firestore_collection())
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

    if dbt_command.elementary:
        return {
            "uuid": request_uuid,
            "links": [
                FollowUpLink("run_status", f"{dbt_command.server_url}job/{request_uuid}"),
                FollowUpLink("last_logs", f"{dbt_command.server_url}job/{request_uuid}/last_logs"),
                FollowUpLink("report", f"{dbt_command.server_url}job/{request_uuid}/report"),
            ]
        }

    return {
        "uuid": request_uuid,
        "links": [
            FollowUpLink("run_status", f"{dbt_command.server_url}job/{request_uuid}"),
            FollowUpLink("last_logs", f"{dbt_command.server_url}job/{request_uuid}/last_logs"),
        ]
    }


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
    job.template.template.max_retries = 0
    job.template.template.containers = [task_container]
    job.template.template.service_account = SERVICE_ACCOUNT

    request = run_v2.CreateJobRequest(
        parent=job_parent,
        job=job,
        job_id=job_id,
    )
    try:
        operation = client.create_job(request=request)
    except Exception:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=400, detail="Cloud Run job creation failed" + traceback_str)

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

    try:
        client.run_job(request=request)
    except Exception:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=400, detail="Cloud Run job start failed" + traceback_str)
    state.run_status = "running"


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_status(uuid: str):
    job_state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())
    run_status = job_state.run_status
    return {"run_status": run_status}


@app.get("/job/{uuid}/last_logs", status_code=status.HTTP_200_OK)
def get_last_logs(uuid: str):
    job_state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())
    logs = job_state.get_last_logs()
    return {"run_logs": logs}


@app.get("/job/{uuid}/logs", status_code=status.HTTP_200_OK)
def get_all_logs(uuid: str):
    job_state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())
    logs = job_state.get_all_logs()
    return {"run_logs": logs}


@app.get("/job/{uuid}/report", status_code=status.HTTP_200_OK)
def get_report(uuid: str):
    state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())
    cloud_storage_folder = state.cloud_storage_folder

    google_console_url = 'https://console.cloud.google.com/storage/browser/_details'
    report_url = f'{google_console_url}/{BUCKET_NAME}/{cloud_storage_folder}/elementary_report.html'
    # return {"url": report_url}

    return RedirectResponse(url=report_url, status_code=status.HTTP_302_FOUND)


@app.get("/check", status_code=status.HTTP_200_OK)
def check():
    print(int(os.environ.get("PORT", 8001)))
    return {
        "response": "Running dbt-server on port "+PORT,
        }


def base64_decode_dbt_command(dbt_command: DbtCommand) -> DbtCommand:
    dbt_command_dict = dbt_command.__dict__
    b64_encoded_keys = ['manifest', 'dbt_project', 'profiles', 'seeds', 'packages']

    for key in b64_encoded_keys:
        value = dbt_command_dict[key]

        if value is None:
            continue

        elif isBase64(value):
            dbt_command_dict[key] = base64.b64decode(value)

        elif isinstance(value, dict):  # seeds will be a dict of b64 encoded seeds files
            for k, v in value.items():
                if isBase64(v):
                    dbt_command_dict[key][k] = base64.b64decode(v)

        else:
            print(f"Warning: {key} is not base64 encoded. It will be passed as is to the cloud run job.")

    return dbt_command


def isBase64(s):
    try:
        return base64.b64encode(base64.b64decode(s)) == bytes(s, 'ascii')
    except Exception:
        return False


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=int(os.environ.get("PORT", 8001)),
        host="0.0.0.0",
        reload=True)
