import os
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest

from utils import parse_manifest_from_json
from state import State
from lab_logger import logging


logger = logging.getLogger(__name__)


def logger_version_callback(event: EventMsg):
    match event.info.level:
        case "debug":
            logger.debug(event.info.msg)
        case "info":
            logger.info(event.info.msg)
        case "warn":
            logger.warn(event.info.msg)
        case "error":
            logger.error(event.info.msg)


def run_job(manifest_json, state: State, dbt_command: str):

    state.status = "running"

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_version_callback])

    # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    cli_args = dbt_command.split(' ')
    logger.info("cli args: {args}".format(args=cli_args))

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not (res.success):
        state.status = "failed"
        handle_exception(res)

    results = res.result
    state.status = "success"
    return results


def handle_exception(res: dbtRunnerResult):
    logger.error({"error": res})
    if res.exception is not None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


if __name__ == '__main__':

    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    dbt_command = os.environ.get("DBT_COMMAND")
    # ex dbt_command = "dbt run --select test"
    request_uuid = os.environ.get("UUID")

    dbt_command.replace("dbt ", "")
    if "--profiles-dir" not in dbt_command:
        dbt_command += " --profiles-dir ."
    if "--log-format" not in dbt_command:
        dbt_command = "--log-format json "+dbt_command

    state = State(request_uuid)

    # we load manifest.json, profiles.yml and dbt_project.yml locally for dbt
    state.get_context_to_local()

    # we extract the manifest
    f = open('manifest.json', 'r')
    manifest = json.loads(f.read())
    f.close()

    results = run_job(manifest, state, dbt_command)
