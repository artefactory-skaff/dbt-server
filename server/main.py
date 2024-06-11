import json
import queue
import threading

import uvicorn
from fastapi import FastAPI, Form, UploadFile, File, status
from fastapi.responses import StreamingResponse, JSONResponse

from server.config import CONFIG
from server.lib.artifacts import generate_id, unpack_artifact
from server.lib.dbt_executor import DBTExecutor
from server.lib.logger import get_logger
from server.lib.models import ServerRuntimeConfig
from server.version import __version__

logger = get_logger(CONFIG.log_level)
schedule_backend = None  # should be different based on CONFIG.provider

app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version=__version__,
    docs_url="/docs"
)


@app.post("/api/run")
async def create_run(
        dbt_runtime_config: str = Form(...),
        server_runtime_config: str = Form("{}"),
        dbt_remote_artifacts: UploadFile = File(...)
):
    """
    if a schedule is given:
        - we upload the data into the scheduled run namespace via storage backend (after unzip)
        - create a scheduler
        - return schedule info and other metadata to user (+ route to trigger schedule in case of dev)
    if no schedule is given (default to @now):
        - we upload the data into the static run namespace via storage backend (after unzip)
        - we execute it locally
        - we save output manifest via storage backend
        - we return state of run (⚠︎ how to handle log)
    """
    dbt_runtime_config = json.loads(dbt_runtime_config)
    flags = dbt_runtime_config["flags"]
    command = dbt_runtime_config["command"]

    server_runtime_config = ServerRuntimeConfig(**json.loads(server_runtime_config))
    if server_runtime_config.is_static_run:
        run_id = generate_id()
        logger.info("Creating static run")
        logger.debug("Unzipping artifact files %s", dbt_remote_artifacts.filename)
        local_artifact_path = await unpack_artifact(
            dbt_remote_artifacts,
            CONFIG.persisted_dir / run_id / "artifacts" / "input"
        )
        logger.debug("Uploading input artifacts to storage backend")
        dbt_executor = DBTExecutor(
            dbt_runtime_config=flags,
            artifact_input=local_artifact_path,
        )
        log_queue = queue.Queue()
        logger.debug("Running dbt command locally")

        thread = threading.Thread(target=dbt_executor.execute_command,
                                  args=(command, log_queue,))
        thread.start()

        def iter_queue():
            while True:
                message = json.loads(log_queue.get())
                _message = json.dumps(message) + '\n'
                yield _message
                if message["info"]["name"] == "CommandCompleted":
                    return

        logger.debug("Uploading output artifacts to storage backend")
        return StreamingResponse(iter_queue(), status_code=status.HTTP_201_CREATED)

    else:
        scheduled_run_id = generate_id("schedule-")
        logger.info("Creating scheduled run")
        logger.debug("Unzipping artifacts")
        await unpack_artifact(
            dbt_remote_artifacts,
            CONFIG.persisted_dir / scheduled_run_id / "artifacts" / "input"
        )
        logger.debug("Uploading artifacts to storage backend")
        logger.debug("Creating scheduler")
        return JSONResponse(status_code=status.HTTP_201_CREATED,
                            content={"type": "scheduled", "schedule_id": scheduled_run_id})


@app.get("/api/run/{run_id}")
def get_run(run_id: str):
    logger.info("Retrieving artifacts from storage backend")
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "in progress", "run_id": run_id})


# @app.post("/api/schedule/{schedule_run_id}/trigger")
# def trigger_scheduled_run(schedule_run_id: str):
#     logger.info("Starting run from schedule %s", schedule_run_id)
#     logger.debug("Creating static run from scheduled run")
#     run_id = generate_id()
#     storage_backend.copy_directory(
#         Path("schedules") / schedule_run_id / "artifacts" / "input",
#         Path("runs") / run_id / "artifacts" / "input"
#     )  # TO FINISH
#     artifact_input = storage_backend.download_directory(
#         Path("runs") / run_id / "artifacts" / "input",
#         CONFIG.persisted_dir / run_id / "artifacts" / "input"
#     )  # TO FINISH
#     dbt_executor = DBTExecutor(
#         dbt_runtime_config=None,
#         artifact_input=artifact_input,
#     )
#     # dbt_executor.execute_command("run", None)
#     # self.storage_backend.persist_directory(
#     #     self.LOCAL_DATA_DIR / run_id / "artifacts" / "output",
#     #     destination_prefix=f"runs/{run_id}/artifacts/output"
#     # )
#     return JSONResponse(status_code=status.HTTP_200_OK, content={"schedule_run_id": schedule_run_id})


@app.get("/api/version")
async def get_version():
    return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})


@app.get("/api/check")
async def check():
    return {"response": f"Running dbt-server version {__version__}"}


if __name__ == '__main__':
    uvicorn.run("server.main:app", host="0.0.0.0", port=CONFIG.port, reload=True)
