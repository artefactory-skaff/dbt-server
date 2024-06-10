import json
import logging
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Any

import uvicorn
from fastapi import FastAPI, status, UploadFile, Form, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator, computed_field
from snowflake import SnowflakeGenerator

__version__ = "0.0.1"  # TODO: handle this properly

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


@dataclass
class RunCommandParameters:
    dbt_runtime_config: Union[str, dict] = Form(...)
    server_runtime_config: Union[str, dict] = Form(...)


class DBTServer:
    LOCAL_DATA_DIR = Path(__file__).parent.parent / "data"

    def __init__(self, logger: logging.Logger, port: int, storage_backend: StorageBackend, schedule_backend=None):

        self.app = get_app()
        self.logger = logger
        self.port = port
        self.storage_backend = storage_backend
        self.schedule_backend = schedule_backend
        self.id_generator = SnowflakeGenerator(instance=1)

    def start(self, reload: bool = False):
        self.__setup_api_routes()
        uvicorn.run("server.lib.dbt_server:get_app", host="0.0.0.0", port=self.port, reload=reload)

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
            dbt_runtime_config = DBTRuntimeConfig(**json.loads(dbt_runtime_config))
            server_runtime_config = ServerRuntimeConfig(**json.loads(server_runtime_config))
            if server_runtime_config.is_static_run:
                run_id = self.__generate_id()
                self.logger.info("Creating static run")
                self.logger.debug("Unzipping artifact files %s", dbt_remote_artifacts.filename)
                local_artifact_path = await self.unpack_artifact(
                    dbt_remote_artifacts,
                    self.LOCAL_DATA_DIR / run_id / "artifacts" / "input"
                )
                self.logger.debug("Uploading input artifacts to storage backend")
                self.storage_backend.persist_directory(
                    source_directory=local_artifact_path,
                    destination_prefix=f"runs/{run_id}/artifacts/input"
                )
                self.logger.debug("Running dbt command locally")
                self.executing_command(
                    dbt_runtime_config=dbt_runtime_config,
                    artifact_input=local_artifact_path
                )
                self.logger.debug("Uploading output artifacts to storage backend")
                self.storage_backend.persist_directory(
                    self.LOCAL_DATA_DIR / run_id / "artifacts" / "output",
                    destination_prefix=f"runs/{run_id}/artifacts/output"
                )
                return JSONResponse(status_code=status.HTTP_200_OK, content={"type": "static", "run_id": run_id})
            else:
                scheduled_run_id = self.__generate_id("schedule-")
                self.logger.info("Creating scheduled run")
                self.logger.debug("Unzipping artifacts")
                local_artifact_path = await self.unpack_artifact(
                    dbt_remote_artifacts,
                    self.LOCAL_DATA_DIR / scheduled_run_id / "artifacts" / "input"
                )
                self.logger.debug("Uploading artifacts to storage backend")
                self.storage_backend.persist_directory(
                    source_directory=local_artifact_path,
                    destination_prefix=f"schedules/{scheduled_run_id}/artifacts/input"
                )
                self.logger.debug("Creating scheduler")
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
            self.executing_command(dbt_runtime_config=None, artifact_input=artifact_input)
            self.storage_backend.persist_directory(
                self.LOCAL_DATA_DIR / run_id / "artifacts" / "output",
                destination_prefix=f"runs/{run_id}/artifacts/output"
            )
            return JSONResponse(status_code=status.HTTP_201_CREATED, content={"schedule_run_id": schedule_run_id})

        @self.app.get("/api/version")
        async def get_version():
            return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})

        @self.app.get("/api/check")
        async def check():
            return {"response": f"Running dbt-server"}

    async def unpack_artifact(self, artifact_file: tempfile.SpooledTemporaryFile, destination_directory: Path) -> Path:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            artifacts_zip_path = temp_dir_path / artifact_file.filename
            with artifacts_zip_path.open('wb') as f:
                contents = await artifact_file.read()
                f.write(contents)
            with zipfile.ZipFile(artifacts_zip_path, 'r') as zip_ref:
                zip_ref.extractall(destination_directory)
            artifacts_zip_path.unlink()
        return destination_directory

    def executing_command(self, dbt_runtime_config: DBTRuntimeConfig, artifact_input: Path):
        self.logger.info("Executing dbt command with artifact input %s", artifact_input.as_posix())

    def __generate_id(self, prefix: str = ""):
        id = next(self.id_generator)
        return f"{prefix}{id}"


def get_app():
    return FastAPI(
        title="dbt-server",
        description="A server to run dbt commands in the cloud",
        version=__version__,
        docs_url="/docs"
    )
