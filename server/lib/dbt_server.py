from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import queue
import concurrent.futures
import tempfile
import threading
import time
import zipfile
from pathlib import Path
from typing import Optional, Any

import uvicorn
from fastapi import FastAPI, status, UploadFile, Form, File
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, field_validator, computed_field
from snowflake import SnowflakeGenerator

__version__ = "0.0.1"  # TODO: handle this properly

from server.lib.dbt_executor import DBTExecutor
from server.lib.storage.base import StorageBackend


class DBTRuntimeConfig(BaseModel):
    param_1: Optional[str] = "value_1"


class ServerRuntimeConfig(BaseModel):
    cron_schedule: Optional[str] = "@now"

    @field_validator("cron_schedule")
    def validate_cron_expression(cls, cron_value: Any):
        # TODO: add validation
        return cron_value

    @computed_field
    def is_static_run(self) -> bool:
        return self.cron_schedule == "@now"


class DBTServer:
    LOCAL_DATA_DIR = Path(__file__).parent.parent.parent / "dbt-server-volume" / "runs"
    CMD_REQUIRES_DEPS = ["run", "build", "test", "seed"]

    def __init__(self, logger: logging.Logger, port: int, storage_backend: StorageBackend, schedule_backend=None):

        self.app = FastAPI(
            title="dbt-server",
            description="A server to run dbt commands in the cloud",
            version=__version__,
            docs_url="/docs"
        )
        self.logger = logger
        self.port = port
        self.storage_backend = storage_backend
        self.schedule_backend = schedule_backend
        self.id_generator = SnowflakeGenerator(instance=1)

    def start(self):
        self.__setup_api_routes()
        uvicorn.run(self.app, host="0.0.0.0", port=self.port)

    def __setup_api_routes(self):
        @self.app.post("/api/run")
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
                run_id = self.__generate_id()
                self.logger.info("Creating static run")
                local_artifact_path = self.LOCAL_DATA_DIR / run_id / "artifacts" / "input"

                self.unzip_and_upload_artifacts(dbt_remote_artifacts, local_artifact_path)

                dbt_executor = DBTExecutor(
                    dbt_runtime_config=flags,
                    artifact_input=local_artifact_path,
                    logger=self.logger
                )
                log_queue = queue.Queue()
                self.logger.info("Running dbt command locally")

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

                self.logger.info("Uploading output artifacts to storage backend")
                return StreamingResponse(iter_queue(), status_code=status.HTTP_200_OK, media_type="text/event-stream")

            else:
                scheduled_run_id = self.__generate_id("schedule-")
                self.logger.info("Creating scheduled run")
                self.unzip_and_upload_artifacts(dbt_remote_artifacts, self.LOCAL_DATA_DIR / scheduled_run_id / "artifacts" / "input")
                self.logger.debug("Creating scheduler")
                # TODO: create scheduler
                return JSONResponse(status_code=status.HTTP_201_CREATED,
                                    content={"type": "scheduled", "schedule_id": scheduled_run_id})

        @self.app.get("/api/run/{run_id}")
        def get_run(run_id: str):
            self.logger.info("Retrieving artifacts from storage backend")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "in progress", "run_id": run_id})

        @self.app.post("/api/schedule/{schedule_run_id}/trigger")
        def trigger_scheduled_run(schedule_run_id: str):
            self.logger.info("Starting run from schedule %s", schedule_run_id)
            self.logger.debug("Creating static run from scheduled run")
            run_id = self.__generate_id()
            self.storage_backend.copy_directory(
                Path("schedules") / schedule_run_id / "artifacts" / "input",
                Path("runs") / run_id / "artifacts" / "input"
            )
            artifact_input = self.storage_backend.download_directory(
                Path("runs") / run_id / "artifacts" / "input",
                self.LOCAL_DATA_DIR / run_id / "artifacts" / "input"
            )
            dbt_executor = DBTExecutor(
                dbt_runtime_config=None,
                artifact_input=artifact_input,
                logger=self.logger
            )
            return JSONResponse(status_code=status.HTTP_200_OK, content={"schedule_run_id": schedule_run_id})

        @self.app.get("/api/version")
        async def get_version():
            return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})

        @self.app.get("/api/check")
        async def check():
            return {"response": f"Running dbt-server"}


    def __generate_id(self, prefix: str = ""):
        id = next(self.id_generator)
        return f"{prefix}{id}"


    async def unzip_and_upload_artifacts(self, artifact_file: UploadFile, destination: Path):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.logger.info(f"Unzipping artifact files {artifact_file.filename}")
            start = time.time()
            temp_dir_path = Path(temp_dir)
            local_artifact_path = await self.unpack_artifact(
                artifact_file,
                temp_dir_path
            )
            self.logger.info(f"Unzipping artifact files took {time.time() - start} seconds")

            files = [item for item in local_artifact_path.rglob("*") if item.is_file()]
            self.logger.info(f"Uploading {len(files)} artifact files to storage backend")
            start = time.time()
            upload_many(files, destination)
            self.logger.info(f"Artifact files uploaded to storage backend in {time.time() - start} seconds")


    @staticmethod
    async def unpack_artifact(artifact_file: tempfile.SpooledTemporaryFile, destination_directory: Path) -> Path:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            artifacts_zip_path = temp_dir_path / artifact_file.filename
            with artifacts_zip_path.open("wb") as f:
                contents = await artifact_file.read()
                f.write(contents)
            with zipfile.ZipFile(artifacts_zip_path, "r") as zip_ref:
                zip_ref.extractall(destination_directory)
            artifacts_zip_path.unlink()
        return destination_directory


def upload_many(files, destination, deadline = None, raise_exception = False, max_workers = 32):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file in files:
            futures.append(executor.submit(
                upload,
                file,
                destination,
            ))
        concurrent.futures.wait(
            futures, timeout=deadline, return_when=concurrent.futures.ALL_COMPLETED
        )

    results = []
    for future in futures:
        exp = future.exception()

        # If raise_exception is False, don't call future.result()
        if exp and not raise_exception:
            results.append(exp)
        # Get the real result. If there was an exception not handled above,
        # this will raise it.
        else:
            results.append(future.result())
    return results


def upload(file, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.copy(file, destination)
