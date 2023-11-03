import os
import traceback

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status

from dbt_server.lib.dbt_cloud_run_job import DbtCloudRunJobStarter, DbtCloudRunJobConfig, DbtCloudRunJobCreationFailed, DbtCloudRunJobStartFailed
from dbt_server.lib.dbt_command import DbtCommand
from dbt_server.lib.state import State
from dbt_server.lib.logger import DbtLogger


DOCKER_IMAGE = os.getenv("DOCKER_IMAGE")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PORT = os.environ.get("PORT", "8001")

app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version="0.0.1",
    docs_url="/docs"
)

@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand = Depends()):
    logger = DbtLogger(server=True)
    logger.log("INFO", f"Received command: {dbt_command.user_command}")

    state = State(dbt_command)
    logger.log("INFO", f"Assigned job id: '{state.uuid}'")
    logger.state = state
    state.extract_artifacts(dbt_command.zipped_artifacts)

    try:
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

    return {
        "uuid": state.uuid,
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
