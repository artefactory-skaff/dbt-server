
from fastapi import FastAPI, status
from pydantic import BaseModel
from google.cloud import run_v2
import os,uuid
import uvicorn
from dotenv import load_dotenv
from pathlib import Path
from datetime import date

from firestore import set_status
from utils import parse_args
from cloud_storage import write_to_bucket
from metadata import get_project_id,get_location,get_service_account

BUCKET_NAME=os.getenv('BUCKET_NAME')
DOCKER_IMAGE=os.getenv('DOCKER_IMAGE')
SERVICE_ACCOUNT=get_service_account()
PROJECT_ID=get_project_id()
LOCATION=get_location()

app = FastAPI()


class dbt_command(BaseModel):
    command: str
    args: dict[str, str] = None
    manifest: str
    dbt_project: str
    profiles: str

@app.get("/metadata")
def get_metadata():
    project_id = get_project_id()
    location = get_location()
    return {"project_id": project_id, "location": location} 

@app.get("/service-account")
def get_metadata():
    service_account = get_service_account()
    return {"service_account": service_account} 



@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: dbt_command):
    print("Received command '{main_command}' and args {args}".format(main_command=dbt_command.command,args=dbt_command.args))

    request_uuid = str(uuid.uuid4())
    set_status(request_uuid,"pending")

    start_cloud_run_job(dbt_command,request_uuid)
    return {"uuid": request_uuid}


def start_cloud_run_job(dbt_command: dbt_command, request_uuid: str):
    print("Starting cloud run job {uuid} with command '{main_command}' and args {args}".format(uuid=request_uuid,main_command=dbt_command.command,args=dbt_command.args))

    set_status(request_uuid,"running")

    load_context_files(dbt_command,request_uuid)

    response_job = create_job(dbt_command,request_uuid)
    launch_job(response_job)


def load_context_files(dbt_command: dbt_command, request_uuid: str):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    bucket_folder = today_str+"-"+request_uuid
    write_to_bucket(BUCKET_NAME,bucket_folder+"/manifest.json",dbt_command.manifest)
    write_to_bucket(BUCKET_NAME,bucket_folder+"/dbt_project.yml",dbt_command.dbt_project)
    write_to_bucket(BUCKET_NAME,bucket_folder+"/profiles.yml",dbt_command.profiles)


def create_job(dbt_command: dbt_command, request_uuid: str):
    # Create a client
    client = run_v2.JobsClient()
    cli_args = [dbt_command.command] + parse_args(dbt_command.args)
    task_container = {
        "image":DOCKER_IMAGE,
        "env":[
            {"name":"DBT_COMMAND","value":' '.join(cli_args)},
            {"name":"UUID","value":request_uuid},
            {"name":"SCRIPT","value":"job.py"}
            ]
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
    uvicorn.run("dbt-server:app", port=int(os.environ.get("PORT", 8001)), host="0.0.0.0", reload=True)
