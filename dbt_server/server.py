import os
import traceback

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from cron_descriptor import get_description

from dbt_server.lib.dbt_cloud_run_job import DbtCloudRunJobStarter, DbtCloudRunJobConfig, DbtCloudRunJobCreationFailed, DbtCloudRunJobStartFailed
from dbt_server.lib.dbt_command import DbtCommand, ScheduledDbtCommand
from dbt_server.lib.cloud_scheduler import CloudScheduler, SchedulerHTTPJobSpec
from dbt_server.lib.state import State
from dbt_server.lib.logger import DbtLogger


DOCKER_IMAGE = os.getenv("DOCKER_IMAGE")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PORT = os.environ.get("PORT", "8001")
SCHEDULED_JOB_DESC_PREFIX = "[dbt-server job] "

app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version="0.0.1",
    docs_url="/docs"
)

@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand = Depends()):
    try:
        logger = DbtLogger(server=True)
        logger.log("INFO", f"Received command: {dbt_command.user_command}")

        state = State(dbt_command)
        logger.log("INFO", f"Assigned job id: '{state.uuid}'")
        logger.state = state
        state.extract_artifacts(dbt_command.zipped_artifacts)

        job_conf = DbtCloudRunJobConfig(
            uuid=state.uuid,
            dbt_command=dbt_command.user_command,
            project_id=PROJECT_ID,
            location=LOCATION,
            service_account=SERVICE_ACCOUNT,
            job_docker_image=DOCKER_IMAGE,
            artifacts_bucket_name=BUCKET_NAME
        )
        DbtCloudRunJobStarter(job_conf, logger).start()

    except (DbtCloudRunJobCreationFailed, DbtCloudRunJobStartFailed) as e:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=400, detail=f"{e.args[0]}\n{traceback_str}")

    except Exception as e:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e.args[0]}\n{traceback_str}")

    return {
        "uuid": state.uuid,
        "message": f"Job created with uuid: {state.uuid}",
        "links": {
            "run_status": f"{dbt_command.server_url}job/{state.uuid}",
            "last_logs": f"{dbt_command.server_url}job/{state.uuid}/last_logs",
        }
    }


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_status(uuid: str):
    job_state = State.from_uuid(uuid)
    run_status = job_state.run_status
    return {"run_status": run_status}


@app.get("/job/{uuid}/last_logs", status_code=status.HTTP_200_OK)
def get_last_logs(uuid: str):
    job_state = State.from_uuid(uuid)
    logs = job_state.get_last_logs()
    run_status = job_state.run_status
    return {"run_logs": logs, "run_status": run_status, "uuid": uuid}


@app.get("/job/{uuid}/logs", status_code=status.HTTP_200_OK)
def get_all_logs(uuid: str):
    job_state = State.from_uuid(uuid)
    logs = job_state.get_all_logs()
    run_status = job_state.run_status
    return {"run_logs": logs, "run_status": run_status, "uuid": uuid}


@app.post("/schedule", status_code=status.HTTP_201_CREATED)
def schedule_run(scheduled_dbt_command: ScheduledDbtCommand = Depends()):
    logger = DbtLogger(server=True)
    logger.log("INFO", f"Received scheduled command: {scheduled_dbt_command.user_command}")

    state = State(scheduled_dbt_command)
    logger.log("INFO", f"Assigned job id: '{state.uuid}'")
    logger.state = state
    state.extract_artifacts(scheduled_dbt_command.zipped_artifacts)

    scheduler = CloudScheduler(project_id=PROJECT_ID, location=LOCATION, service_account_email=SERVICE_ACCOUNT)
    job_to_schedule = SchedulerHTTPJobSpec(
        job_name=f"dbt-server-{state.uuid}" if scheduled_dbt_command.schedule_name is None else scheduled_dbt_command.schedule_name,
        schedule=scheduled_dbt_command.schedule,
        target_uri=f"{scheduled_dbt_command.server_url}schedule/{state.uuid}/start",
        description=f"{SCHEDULED_JOB_DESC_PREFIX}{scheduled_dbt_command.user_command}"
    )
    scheduler.create_http_scheduled_job(job_to_schedule)


    return {
        "uuid": state.uuid,
        "message": f"Job {scheduled_dbt_command.user_command} scheduled at {scheduled_dbt_command.schedule} ({get_description(scheduled_dbt_command.schedule)}) with uuid: {state.uuid}",
        "links": {
            "start": f"{scheduled_dbt_command.server_url}schedule/{state.uuid}/start",
        }
    }

@app.get("/schedule", status_code=status.HTTP_200_OK)
def list_schedules():
    scheduler = CloudScheduler(project_id=PROJECT_ID, location=LOCATION, service_account_email=SERVICE_ACCOUNT)
    schedules = scheduler.list()

    return {
        "schedules": {
            schedule.name.split("/")[-1]: {
                "name": schedule.name.split("/")[-1],
                "command": schedule.description.replace(SCHEDULED_JOB_DESC_PREFIX, ""),
                "schedule": schedule.schedule,
                "timezone": schedule.time_zone,
                "target": schedule.http_target.uri,
            }
            for schedule in schedules if schedule.description.startswith(SCHEDULED_JOB_DESC_PREFIX) and schedule.state.name == "ENABLED"
        }
    }

@app.delete("/schedule/{name}", status_code=status.HTTP_200_OK)
def list_schedules(name):
    scheduler = CloudScheduler(project_id=PROJECT_ID, location=LOCATION, service_account_email=SERVICE_ACCOUNT)
    deleted = scheduler.delete(name)

    return {
        "message": f"Schedule {name} deleted" if deleted else f"Nothing to delete, schedule {name} does not exist or is disabled in {PROJECT_ID}/{LOCATION}",
    }

@app.post("/schedule/{uuid}/start", status_code=status.HTTP_200_OK)
def start_scheduled_run(uuid: str):
    state = State.from_schedule_uuid(uuid)
    logger = DbtLogger(server=True)
    logger.state = state
    logger.log("INFO", f"Assigned job id: '{state.uuid}'")
    logger.log("INFO", f"Starting scheduled job with command: {state.user_command}")

    try:
        job_conf = DbtCloudRunJobConfig(
            uuid=state.uuid,
            dbt_command=state.user_command,
            project_id=PROJECT_ID,
            location=LOCATION,
            service_account=SERVICE_ACCOUNT,
            job_docker_image=DOCKER_IMAGE,
            artifacts_bucket_name=BUCKET_NAME
        )
        DbtCloudRunJobStarter(job_conf, logger).start()
    except (DbtCloudRunJobCreationFailed, DbtCloudRunJobStartFailed) as e:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=400, detail=f"{e.args[0]}\n{traceback_str}")

    return {
        "uuid": state.uuid,
        "message": f"Job created with uuid: {state.uuid}",
    }


@app.get("/check", status_code=status.HTTP_200_OK)
def check():
    return { "response": f"Running dbt-server on port {PORT}"}


if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        port=int(PORT),
        host="0.0.0.0",
        reload=True
    )
