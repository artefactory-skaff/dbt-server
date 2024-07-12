import asyncio
import json
from pathlib import Path
import time
from typing import Callable
import humanize

from skaff_telemetry import skaff_telemetry
import uvicorn
from fastapi import APIRouter, FastAPI, Form, Request, UploadFile, File, status, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from jinja2 import Template

from dbtr.server.config import CONFIG
from dbtr.server.lib.artifacts import move_folder, unzip_and_persist_artifacts
from dbtr.server.lib.database import Database
from dbtr.server.lib.dbt_executor import DBTExecutor
from dbtr.server.lib.lock import Lock, LockException, LockNotFound
from dbtr.server.lib.logger import get_logger
from dbtr.server.lib.models import ServerJob
from dbtr.server.lib.scheduler.base import BaseScheduler, get_scheduler
from dbtr.server.version import __version__


logger = get_logger(CONFIG.log_level)
scheduling_backend: BaseScheduler = get_scheduler(CONFIG.provider)


app = FastAPI(
    title="dbt-server",
    description="A server to run dbt commands in the cloud",
    version=__version__,
    docs_url="/docs"
)
router = APIRouter()


with Database(CONFIG.db_connection_string, logger=logger) as db:
    db.initialize_schema()


@app.middleware("http")
async def telemetry_middleware(request: Request, call_next):
    @skaff_telemetry(accelerator_name="dbtr-server", function_name=request.url.path, version_number=__version__,
                     project_name='')
    async def perform_request(request):
        return await call_next(request)

    response = await perform_request(request)
    return response


@app.post("/api/run")
async def create_run(
    background_tasks: BackgroundTasks,
    server_runtime_config: str = Form({}),
    dbt_remote_artifacts: UploadFile = File(...),
):
    server_runtime_config, local_artifacts_path = await unpack_job_request(server_runtime_config, dbt_remote_artifacts)

    if server_runtime_config.run_now:
        result = await start_dbt_job(server_runtime_config, local_artifacts_path, background_tasks)
        status_code = status.HTTP_200_OK
    else:
        result = await schedule_dbt_job(server_runtime_config)
        status_code = status.HTTP_201_CREATED

    return JSONResponse(status_code=status_code, content=result)


@app.get("/api/run/{run_id}")
def get_run(run_id: str):
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        query = """
            SELECT rc.run_id, rc.run_conf_version, rc.project, rc.server_url, rc.cloud_provider, rc.provider_config, rc.requester, rc.dbt_runtime_config, rc.schedule_cron, rc.schedule_name, rc.schedule_description, r.run_status, r.start_time, r.end_time
            FROM RunConfiguration rc
            JOIN Runs r ON rc.run_id = r.run_id
            WHERE rc.run_id = ?
        """
        run = db.fetchone(query, (run_id,))
    return JSONResponse(status_code=status.HTTP_200_OK, content=sanitize_run_from_db(run))


@app.get("/api/run")
def list_runs(skip: int = 0, limit: int = 20, project: str = None):
    logger.info("Listing past runs")
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        base_query = """
            SELECT rc.run_id, rc.run_conf_version, rc.project, rc.server_url, rc.cloud_provider, rc.provider_config, rc.requester, rc.dbt_runtime_config, rc.schedule_cron, rc.schedule_name, rc.schedule_description, r.run_status, r.start_time, r.end_time
            FROM RunConfiguration rc
            JOIN Runs r ON rc.run_id = r.run_id
            WHERE rc.schedule_cron IS NULL
            {% if project %}
            AND rc.project = :project
            {% endif %}
            ORDER BY rc.run_id DESC
            LIMIT :limit OFFSET :skip
        """
        template = Template(base_query)
        query = template.render(project=project)
        params = {"limit": limit, "skip": skip}
        if project:
            params["project"] = project
        runs = db.fetchall(query, params)

    runs_dict = {}
    for run in runs:
        run = sanitize_run_from_db(run)
        run_id = run["run_id"]
        runs_dict[run_id] = run
    return JSONResponse(status_code=status.HTTP_200_OK, content=runs_dict)


@router.get("/api/run/{run_id}/docs/{file_path:path}")
async def serve_docs(run_id: str, file_path: str):
    docs_dir = CONFIG.persisted_dir / "runs" / run_id / "artifacts" / "output" / "docs"
    file_location = docs_dir / file_path
    if file_location.exists():
        return FileResponse(file_location)
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "File not found"})


@router.get("/api/run/{run_id}/elementary/{file_path:path}")
async def serve_elementary(run_id: str, file_path: str):
    elementary_dir = CONFIG.persisted_dir / "runs" / run_id / "artifacts" / "output" / "elementary"
    file_location = elementary_dir / file_path
    if file_location.exists():
        return FileResponse(file_location)
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "File not found"})


@app.post("/api/schedule/{scheduled_run_id}/trigger")
async def trigger_scheduled_run(scheduled_run_id: str, background_tasks: BackgroundTasks):
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        server_runtime_config = ServerJob.from_scheduled_run(db, scheduled_run_id)
        server_runtime_config.to_db(db)

    local_artifact_path = move_folder(
        CONFIG.persisted_dir / Path("schedules") / scheduled_run_id,
        CONFIG.persisted_dir / Path("runs") / server_runtime_config.run_id,
        delete_after_copy=False,
    )

    project_dir = local_artifact_path / "artifacts" / "input"
    return await start_dbt_job(server_runtime_config, project_dir, background_tasks)


@app.get("/api/schedule")
def list_schedules(skip: int = 0, limit: int = 20):
    logger.info("Listing schedules")
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        query = """
            SELECT run_id, run_conf_version, project, server_url, cloud_provider, provider_config, requester, dbt_runtime_config, schedule_cron, schedule_name, schedule_description
            FROM RunConfiguration
            WHERE schedule_cron IS NOT NULL
            ORDER BY run_id DESC
            LIMIT ? OFFSET ?
        """
        schedules = db.fetchall(query, (limit, skip))
    schedules_dict = {}
    for schedule in schedules:
        schedule = sanitize_run_from_db(schedule)
        schedule_name = schedule["schedule_name"]
        schedules_dict[schedule_name] = schedule
    return JSONResponse(status_code=status.HTTP_200_OK, content=schedules_dict)


@app.get("/api/schedule/{schedule_name}")
def get_schedule(schedule_name: str):
    logger.info(f"Fetching schedule with ID: {schedule_name}")
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        query = """
            SELECT run_id, run_conf_version, project, server_url, cloud_provider, provider_config, requester, dbt_runtime_config, schedule_cron, schedule_name, schedule_description
            FROM RunConfiguration
            WHERE schedule_name = ?
        """
        schedule = db.fetchone(query, (schedule_name,))

    if schedule:
        schedule = sanitize_run_from_db(schedule)
        return JSONResponse(status_code=status.HTTP_200_OK, content=schedule)
    else:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Schedule not found"})


@app.delete("/api/schedule/{schedule_name}")
def delete_schedule(schedule_name: str):
    logger.info(f"Deleting schedule: {schedule_name}")

    try:
        scheduling_backend().delete(schedule_name)
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": str(e)})

    with Database(CONFIG.db_connection_string, logger=logger) as db:
        query = """
            DELETE FROM RunConfiguration
            WHERE schedule_name = ?
        """
        result = db.execute(query, (schedule_name,))

    if result.rowcount > 0:
        return JSONResponse(status_code=status.HTTP_200_OK, content={"message": f"Schedule {schedule_name} deleted successfully"})
    else:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Schedule not found"})


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


@app.get("/api/project")
def get_project():
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        query = """
            SELECT DISTINCT project
            FROM RunConfiguration
        """
        projects = db.fetchall(query)

    content = {}
    for project in projects:
        content[project["project"]] = {"name": project["project"]}

    return JSONResponse(status_code=status.HTTP_200_OK, content=content)


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


@app.get("/api/version")
async def get_version():
    return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})


@app.get("/api/check")
async def check():
    return {"response": f"Running dbt-server version {__version__}"}

async def unpack_job_request(server_runtime_config, dbt_remote_artifacts):
    server_runtime_config_dict = json.loads(server_runtime_config)
    server_runtime_config = ServerJob(**server_runtime_config_dict)

    logger.debug("Persisting metadata")
    with Database(CONFIG.db_connection_string, logger=logger) as db:
        server_runtime_config.to_db(db)

    logger.debug(f"Unzipping artifact files {dbt_remote_artifacts.filename}")
    start = time.time()
    local_artifact_path = await unzip_and_persist_artifacts(
        dbt_remote_artifacts,
        CONFIG.persisted_dir / ("runs" if server_runtime_config.run_now else "schedules") / server_runtime_config.run_id / "artifacts" / "input"
    )
    elapsed_time = humanize.naturaldelta(time.time() - start)
    logger.debug(f"Unpacked and persisted artifact in {elapsed_time}")

    return server_runtime_config, local_artifact_path


async def start_dbt_job(server_runtime_config: ServerJob, local_artifact_path: Path, background_tasks: BackgroundTasks):
    try:
        lock = Lock(Database(CONFIG.db_connection_string, logger=logger))
        lock.acquire(holder=server_runtime_config.requester, run_id=server_runtime_config.run_id)
    except LockException as e:
        logger.error(f"Failed to acquire lock: {e}")
        return JSONResponse(
            status_code=status.HTTP_423_LOCKED,
            content={"error": "Run already in progress", "lock_info": str(e.lock_data)}
        )

    dbt_executor = DBTExecutor(
        dbt_runtime_config=server_runtime_config.dbt_runtime_config["flags"],
        server_runtime_config=server_runtime_config,
        artifact_input=local_artifact_path,
        logger=logger
    )

    # The lock is passed to the executor released when the background task is completed
    background_tasks.add_task(dbt_executor.execute_command, server_runtime_config.dbt_runtime_config["command"], lock)

    response_contents = {
        "type": "static",
        "run_id": server_runtime_config.run_id,
        "next_url": f"{server_runtime_config.server_url}/api/logs/{server_runtime_config.run_id}"
    }
    return response_contents


async def schedule_dbt_job(server_runtime_config: ServerJob):
    logger.debug("Creating scheduler")
    trigger_url = f"{server_runtime_config.server_url}/api/schedule/{server_runtime_config.run_id}/trigger"

    scheduling_backend.create_or_update_job(
        name=server_runtime_config.schedule_name,
        cron_expression=server_runtime_config.schedule_cron,
        trigger_url=trigger_url,
        description=server_runtime_config.schedule_description,
    )

    response_contents = {
        "type": "scheduled",
        "schedule_id": server_runtime_config.run_id,
        "schedule_name": server_runtime_config.schedule_name,
        "schedule_cron": server_runtime_config.schedule_cron,
        "next_url": trigger_url,
    }
    return response_contents


async def stream_log_file(run_id: str, filter: Callable, stop: Callable):
    # TODO: we should retrieve the path to the log file from run metadata
    log_file = CONFIG.persisted_dir / "runs" / run_id / "artifacts" / "input" / "logs" / "dbt.log"

    while not log_file.exists():  # TODO: handle queued run to avoid having a blocking operation here
        await asyncio.sleep(0.1)
    with open(log_file, "r") as file:
        file.seek(0)  # This moves the cursor to the beginning of the file
        lock_refresh_cooldown = 10
        last_lock_refresh = 0
        with Database(CONFIG.db_connection_string, logger=logger) as db:
            while True:
                _line = file.readline()
                if _line:
                    if time.time() - last_lock_refresh > lock_refresh_cooldown:
                        last_lock_refresh = time.time()
                        try:
                            lock = Lock.from_db(db)
                            if lock.lock_data.run_id == run_id:
                                lock.refresh()
                        except LockNotFound:
                            pass

                    line = json.loads(_line)
                    if filter(line):
                        yield _line
                    if stop(line):
                        return
                else:
                    await asyncio.sleep(0.1)


def sanitize_run_from_db(run: str):
    run = dict(run)
    run["provider_config"] = json.loads(run["provider_config"])
    run["dbt_runtime_config"] = json.loads(run["dbt_runtime_config"])
    return run


app.include_router(router)


if __name__ == '__main__':
    uvicorn.run("dbtr.server.main:app", host="0.0.0.0", port=CONFIG.port, workers=1, reload=False)
