
from fastapi import FastAPI
from pydantic import BaseModel
from google.cloud import run_v2
import os,uuid
import uvicorn
from dotenv import load_dotenv
from pathlib import Path

from firestore import set_status
from utils import parse_args
from cloud_storage import write_to_bucket

dotenv_path = Path('.env.server')
load_dotenv(dotenv_path=dotenv_path)

BUCKET_NAME=os.getenv('BUCKET_NAME')
DOCKER_IMAGE=os.getenv('DOCKER_IMAGE')
SERVICE_ACCOUNT=os.getenv('SERVICE_ACCOUNT')
PROJECT_ID=os.getenv('PROJECT_ID')
LOCATION=os.getenv('LOCATION')

app = FastAPI()


class dbt_command(BaseModel):
    command: str
    args: dict[str, str] = None
    manifest: str
    dbt_project: str
    profiles: str
    # {"command": "run", "args": {"--select": "vbak"}}


@app.post("/dbt")
def run_command(dbt_command: dbt_command):
    print("command received", dbt_command.command,dbt_command.args)

    request_uuid = str(uuid.uuid4())
    set_status(request_uuid,"pending")

    start_cloud_run_job(dbt_command,request_uuid)
    return {"uuid": request_uuid}


def start_cloud_run_job(dbt_command: dbt_command, request_uuid: str):
    print("async command received", dbt_command.command,dbt_command.args)

    set_status(request_uuid,"running")
    write_to_bucket(BUCKET_NAME,request_uuid+"_manifest.json",dbt_command.manifest)
    write_to_bucket(BUCKET_NAME,request_uuid+"_dbt_project.yml",dbt_command.dbt_project)
    write_to_bucket(BUCKET_NAME,request_uuid+"_profiles.yml",dbt_command.profiles)

    response_job = create_job(request_uuid,dbt_command)
    launch_job(response_job)

    return 0


def create_job(request_uuid,dbt_command):
    # Create a client
    client = run_v2.JobsClient()
    cli_args = [dbt_command.command] + parse_args(dbt_command.args)
    task_container = {
        "image":DOCKER_IMAGE,
        "env":[{"name":"DBT_COMMAND","value":' '.join(cli_args)},{"name":"UUID","value":request_uuid}]
        }
    job_id = "u"+request_uuid.replace('-','') # job_id must start with a letter and cannot contain '-'
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


def launch_job(response_job):
    # Create a client
    client = run_v2.JobsClient()
    job_name = response_job.name
    print("job_name: ",job_name)

    # Initialize request argument(s)
    request = run_v2.RunJobRequest(
        name=job_name,
    )

    # Make the request
    client.run_job(request=request)


if __name__ == "__main__":
    uvicorn.run(app, port=int(os.environ.get("PORT", 8001)), host="0.0.0.0")
