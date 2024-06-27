import asyncio
import json
from pathlib import Path
import time
from typing import Callable

from skaff_telemetry import skaff_telemetry
import uvicorn
from fastapi import FastAPI, Form, Request, UploadFile, File, status, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse

from dbtr.server.config import CONFIG
from dbtr.server.lib.artifacts import fetch_run_config, generate_id, move_folder, persist_run_config, \
    unpack_and_persist_artifact
from dbtr.server.lib.database import Database
from dbtr.server.lib.dbt_executor import DBTExecutor
from dbtr.server.lib.logger import get_logger
from dbtr.server.lib.models import ServerRuntimeConfig
from dbtr.server.version import __version__
from dbtr.server.lib.lock import Lock, LockException


logger = get_logger(CONFIG.log_level)
schedule_backend = None  # should be different based on CONFIG.provider


app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version=__version__,
    docs_url="/docs"
)


with Database(CONFIG.db_connection_string, logger=logger) as db:
    db.initialize_schema()


@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    @skaff_telemetry(accelerator_name="dbtr-server", function_name=request.url.path, version_number=__version__, project_name='')
    async def perform_request(request):
        return await call_next(request)
    response = await perform_request(request)
    return response


@app.post("/api/run")
async def create_run(
        background_tasks: BackgroundTasks,
        dbt_runtime_config: str = Form(...),
        server_runtime_config: str = Form({}),
        dbt_remote_artifacts: UploadFile = File(...),
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
    server_runtime_config = json.loads(server_runtime_config)
    flags = dbt_runtime_config["flags"]
    command = dbt_runtime_config["command"]
    server_runtime_conf = ServerRuntimeConfig(**server_runtime_config)
    if server_runtime_conf.is_static_run:
        run_id = generate_id()
        server_runtime_config["run_id"] = run_id

        try:
            lock = Lock(Database(CONFIG.db_connection_string, logger=logger)).acquire(holder=server_runtime_conf.requester, run_id=run_id)
        except LockException as e:
            logger.error(f"Failed to acquire lock: {e}")
            return JSONResponse(status_code=status.HTTP_423_LOCKED, content={"error": "Run already in progress", "lock_info": str(e.lock_data)})

        logger.info("Creating static run")
        logger.debug("Persisting metadata")
        persist_run_config(dbt_runtime_config, server_runtime_config)
        logger.debug(f"Unzipping artifact files {dbt_remote_artifacts.filename}")

        start = time.time()
        local_artifact_path = await unpack_and_persist_artifact(
            dbt_remote_artifacts,
            CONFIG.persisted_dir / "runs" / run_id / "artifacts" / "input"
        )
        logger.debug(f"Unpacked and persisted artifact in {round(time.time() - start, 1)} seconds")

        dbt_executor = DBTExecutor(
            dbt_runtime_config=flags,
            server_runtime_config=server_runtime_config,
            artifact_input=local_artifact_path,
            logger=logger
        )

        # The lock is released when the background task is completed
        background_tasks.add_task(dbt_executor.execute_command, command, lock)
        return JSONResponse(status_code=status.HTTP_200_OK, content={"run_id": run_id})

    else:
        scheduled_run_id = generate_id("schedule-")
        server_runtime_config["run_id"] = scheduled_run_id

        logger.info("Creating scheduled run")
        logger.debug("Persisting metadata")
        persist_run_config(dbt_runtime_config, server_runtime_config, run_id)
        logger.debug("Unzipping artifacts")
        await unpack_and_persist_artifact(
            dbt_remote_artifacts,
            CONFIG.persisted_dir / "schedules" / scheduled_run_id / "artifacts" / "input"
        )
        logger.debug("Creating scheduler")
        # TODO: scheduling
        return JSONResponse(status_code=status.HTTP_201_CREATED,
                            content={"type": "scheduled", "schedule_id": scheduled_run_id})


@app.get("/api/logs/{run_id}")
def stream_log(run_id: str, include_debug: bool = False):
    return StreamingResponse(
        stream_log_file(
            run_id,
            filter=lambda line: include_debug or line.get("info", {}).get("level") != "debug",
            stop=lambda line: line.get("info", {}).get("name") == "CommandCompleted"
        ),
        media_type="text/event-stream",
        status_code=status.HTTP_200_OK
    )


@app.get("/api/run/{run_id}")
def get_run(run_id: str):
    logger.info("Retrieving artifacts from storage backend")
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "in progress", "run_id": run_id})


@app.get("/api/run")
def list_runs(skip: int = 0, limit: int = 20):
    logger.info("Listing past runs")
    runs_dir = CONFIG.persisted_dir / "runs"
    runs = sorted([run / "metadata.json" for run in runs_dir.iterdir() if run.is_dir()], reverse=True)

    skip = min(max(skip, 0), len(runs))
    paginated_runs = runs[skip: min(skip + limit, len(runs))]

    runs = {}
    for metadata_file in paginated_runs:
        if metadata_file.exists():
            with open(metadata_file, "r") as file:
                metadata = json.load(file)
                run_id = metadata_file.parent.name
                runs[run_id] = metadata
    return JSONResponse(status_code=status.HTTP_200_OK, content=runs)


@app.get("/api/project")
def get_project():
    runs_dir = CONFIG.persisted_dir / "runs"
    project_names = set()

    for run in runs_dir.iterdir():
        if run.is_dir():
            metadata_file = run / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, "r") as file:
                    metadata = json.load(file)
                    project_dir = metadata["dbt_runtime_config"]["flags"]["project_dir"]
                    project_name = project_dir.split("/")[-1]
                    project_names.add(project_name)

    return JSONResponse(status_code=status.HTTP_200_OK, content={"projects": list(project_names)})


@app.post("/api/schedule/{schedule_run_id}/trigger")
def trigger_scheduled_run(schedule_run_id: str, background_tasks: BackgroundTasks):
    # TODO: handle static metadata of a run (dbt_runtime_config)
    logger.info("Starting run started from schedule %s", schedule_run_id)
    logger.debug("Creating static run from scheduled run")
    run_id = generate_id()
    run_input = move_folder(
        CONFIG.persisted_dir / Path("schedules") / schedule_run_id,
        CONFIG.persisted_dir / Path("runs") / run_id
    )
    server_runtime_config = fetch_run_config(run_id)
    dbt_runtime_config = server_runtime_config["dbt_runtime_config"]
    dbt_executor = DBTExecutor(
        dbt_runtime_config=dbt_runtime_config["flags"],
        server_runtime_config=server_runtime_config,
        artifact_input=run_input / "artifacts" / "input",
        logger=logger
    )
    background_tasks.add_task(dbt_executor.execute_command, dbt_runtime_config["command"])
    return JSONResponse(status_code=status.HTTP_200_OK, content={"run_id": run_id})


@app.get("/api/version")
async def get_version():
    return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})


@app.get("/api/check")
async def check():
    return {"response": f"Running dbt-server version {__version__}"}


@app.post("/api/unlock")
def unlock_server():
    try:
        lock = Lock.from_db(Database(CONFIG.db_connection_string, logger=logger))
        lock.release()
        return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Server unlocked successfully"})
    except FileNotFoundError:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Lock not found"})
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": str(e)})



async def stream_log_file(run_id: str, filter: Callable, stop: Callable):
    # TODO: we should retrieve the path to the log file from run metadata
    log_file = CONFIG.persisted_dir / "runs" / run_id / "artifacts" / "input" / "logs" / "dbt.log"

    while not log_file.exists():  # TODO: handle queued run to avoid having a blocking operation here
        await asyncio.sleep(0.1)
    with open(log_file, "r") as file:
        file.seek(0)  # This moves the cursor to the beginning of the file
        while True:
            _line = file.readline()
            if _line:

                with Database(CONFIG.db_connection_string, logger=logger) as db:
                    try:
                        lock = Lock.from_db(db)
                        if lock.lock_data.run_id == run_id:
                            lock.refresh()
                    except FileNotFoundError:
                        pass

                line = json.loads(_line)
                if filter(line):
                    yield _line
                if stop(line):
                    return
            else:
                await asyncio.sleep(0.1)


if __name__ == '__main__':
    uvicorn.run("dbtr.server.main:app", host="0.0.0.0", port=CONFIG.port, workers=1, reload=False)
