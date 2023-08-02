
from fastapi import FastAPI, status
from google.cloud import run_v2
import google.cloud.logging
from google.cloud.logging import DESCENDING
import logging as logger
import os
import uuid
import uvicorn
import re

from utils import dbt_command
from metadata import get_project_id, get_location, get_service_account
from state import State
# from lab_logger import logging

BUCKET_NAME = os.getenv('BUCKET_NAME')
DOCKER_IMAGE = os.getenv('DOCKER_IMAGE')
SERVICE_ACCOUNT = get_service_account()
PROJECT_ID = get_project_id()
LOCATION = get_location()

app = FastAPI()
# logger = logging.getLogger(__name__)

client = google.cloud.logging.Client()
client.setup_logging()


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_state(uuid: str):

    job_state = State(uuid)

    job_name = "u"+uuid.replace('-', '')

    filter_str = (
        # f'logName="projects/{PROJECT_ID}/logs/cloudaudit.googleapis.com%2Factivity"'
        '(resource.type="cloud_run_job" OR resource.type="gce_instance" OR resource.type="cloud_run_revision")'
        ' AND severity >= INFO'
        f' AND resource.labels.job_name="{job_name}"'
    )

    logger.info("filters: "+filter_str)
    entries = client.list_entries(filter_=filter_str, order_by=DESCENDING, max_results=5)
    logs = []
    for log in entries:
        logger.info(str(log))
        logs.append(str(log.payload))
    return {"run_status": job_state.run_status, "entries": logs}


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: dbt_command):
    logger.info("Received command '{command}'".format(
        command=dbt_command.command)
        )

    request_uuid = str(uuid.uuid4())
    state = State(request_uuid)
    state.init_state()
    state.run_status = "pending"

    start_cloud_run_job(dbt_command, state)
    return {"uuid": request_uuid}


def start_cloud_run_job(dbt_command: dbt_command, state: State):
    logger.info(
        "Starting cloud run job {uuid} with command '{main_command}'".format(
            uuid=state.uuid,
            main_command=dbt_command.command)
        )

    state.run_status = "running"
    state.load_context(dbt_command)

    processed_command, _ = process_command(dbt_command.command)
    logger.info('processed command: '+processed_command)

    response_job = create_job(processed_command, state.uuid)
    launch_job(response_job)


def process_command(command: str):
    processed_command = command
    processed_command = processed_command.replace("dbt ", "")

    # handle log settings
    m = re.search('--log-level (.+?)( |$)', processed_command)
    debug_level = False
    if m:  # command contains --log-level <something>
        log_level = m.group(1)
        if log_level == "debug":
            debug_level = True
        else:
            # if not --log-level, we remove it and add --debug
            # to test: processed_command = processed_command.replace(m.group(), "")
            begin, end = m.span()
            if processed_command[end:] != "":
                processed_command = processed_command[:begin] + processed_command[end:]
            else:
                processed_command = processed_command[:begin-1]
            processed_command = "--debug "+processed_command
    else:
        if "--debug" in processed_command:
            debug_level = True
        else:
            processed_command = "--debug "+processed_command

    # add profile-dir
    if "--profiles-dir" not in processed_command:
        processed_command += " --profiles-dir ."

    # add log-format
    if "--log-format" not in processed_command:
        processed_command = "--log-format json "+processed_command

    return processed_command, debug_level


def create_job(command: str, request_uuid: str):
    # Create a client
    client = run_v2.JobsClient()
    task_container = {
        "image": DOCKER_IMAGE,
        "env": [
            {"name": "DBT_COMMAND", "value": command},
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

    logger.info("Waiting for operation to complete...")

    response = operation.result()
    logger.info({"response": str(response)})
    return response


def launch_job(response_job: run_v2.types.Job):
    # Create a client
    client = run_v2.JobsClient()
    job_name = response_job.name
    logger.info("job_name:{job}".format(job=job_name))

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
