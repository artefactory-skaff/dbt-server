import uuid
import uvicorn
import click
from fastapi import FastAPI, status


from dbt_server.config import Settings
from dbt_server.lib.logger import LOGGER
from dbt_server.lib.job import Job, JobFactory
from dbt_server.lib.dbt_classes import DbtCommand, FollowUpLink
from dbt_server.lib.command_processor import process_command
from dbt_server.lib.state import State
from dbt_server.lib.storage import StorageFactory
from dbt_server.lib.metadata_document import MetadataDocumentFactory


settings = Settings()
app = FastAPI()
STORAGE_INSTANCE = StorageFactory().create(settings.storage_service)


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand):
    metadata_document = MetadataDocumentFactory().create(
        settings.metadata_document_service, settings.collection_name, settings.uuid
    )
    state = State(settings.uuid, metadata_document)
    state.run_status = "pending"
    LOGGER.uuid = settings.uuid

    LOGGER.log("INFO", f"Received command '{dbt_command.user_command}'")
    state.user_command = dbt_command.user_command

    processed_command = process_command(dbt_command.user_command)
    LOGGER.log("INFO", f"Processed command: {processed_command}")
    dbt_command.processed_command = processed_command

    state.load_context(dbt_command)

    job = Job(JobFactory().create(settings.job_service))
    job_name = job.create(state, dbt_command)
    job.launch(state, job_name)

    return {
        "uuid": request_uuid,
        "links": [
            FollowUpLink("run_status", f"{dbt_command.server_url}job/{request_uuid}"),
            FollowUpLink(
                "last_logs", f"{dbt_command.server_url}job/{request_uuid}/last_logs"
            ),
        ],
    }


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_state(uuid: str):
    job_state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    run_status = job_state.run_status
    return {"run_status": run_status}


@app.get("/job/{uuid}/last_logs", status_code=status.HTTP_200_OK)
def get_last_logs(uuid: str):
    job_state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    logs = job_state.get_last_logs()
    return {"run_logs": logs}


@app.get("/job/{uuid}/report", status_code=status.HTTP_200_OK)
def get_report(uuid: str):
    state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    storage_folder = state.storage_folder
    report = STORAGE_INSTANCE.get_file(
        settings.bucket_name, storage_folder + "/elementary_report.html"
    )

    url = STORAGE_INSTANCE.get_file_console_url(
        settings.bucket_name, f"{storage_folder}/elementary_report.html"
    )

    return {"url": url}


@app.get("/check", status_code=status.HTTP_200_OK)
def check():
    LOGGER.log("INFO", f"Running dbt-server on port : {settings.port}")
    return {"response": f"Running dbt-server on port {settings.port}"}


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    help="Run dbt commands from the dbt server.",
)
def launch_app():
    uvicorn.run(
        "dbt_server.dbt_server:app",
        port=settings.port,
        host="0.0.0.0",
        reload=True,
    )


if __name__ == "__main__":
    launch_app()
