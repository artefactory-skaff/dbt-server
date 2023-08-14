import os
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest
import google.cloud.logging
# import logging

from utils import parse_manifest_from_json
from state import State
# from lab_logger import logging


client = google.cloud.logging.Client(_use_grpc=False)
client.setup_logging()
logger = client.logger(name=os.environ.get("UUID"))
# logger = logging.getLogger(__name__)


def logger_version_callback(event: EventMsg):
    entry = str(event.info.msg)
    logger.log(
        "dbt event: "+entry,
        severity=event.info.level.capitalize(),
        labels={"uuid": os.environ.get("UUID")},
    )


def run_job(manifest_json, state: State, dbt_command: str):

    state.run_status = "running"

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_version_callback])

    # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    cli_args = dbt_command.split(' ')
    logger.log(
        "cli args: "+str(cli_args),
        severity="INFO",
        labels={"uuid": os.environ.get("UUID")},
    )

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not (res.success):
        state.run_status = "failed"
        handle_exception(res)

    results = res.result
    state.run_status = "success"
    return results


def handle_exception(res: dbtRunnerResult):
    logger.log(
        str(res),
        severity="ERROR",
        labels={"uuid": os.environ.get("UUID")},
    )
    if res.exception is not None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


if __name__ == '__main__':

    logger.log(
        "Job started",
        severity="INFO",
        labels={"uuid": os.environ.get("UUID")},
    )

    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    dbt_command = os.environ.get("DBT_COMMAND")
    request_uuid = os.environ.get("UUID")

    logger.log(
        "DBT_COMMAND: "+dbt_command,
        severity="INFO",
        labels={"uuid": os.environ.get("UUID")},
    )

    state = State(request_uuid)

    # we load manifest.json and dbt_project.yml locally for dbt
    state.get_context_to_local()

    # we extract the manifest
    with open('manifest.json', 'r') as f:
        manifest = json.loads(f.read())

    results = run_job(manifest, state, dbt_command)
