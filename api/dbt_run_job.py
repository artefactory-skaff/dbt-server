import os
import sys
import msgpack
import json

from click.parser import split_arg_string
from dbt.cli.main import dbtRunner, dbtRunnerResult, cli
from dbt.events.base_types import EventMsg
from dbt.events.functions import msg_to_json
from dbt.contracts.graph.manifest import Manifest
from elementary.monitor.cli import report
from fastapi import HTTPException

sys.path.insert(1, './lib')

from state import State
from new_logger import init_logger

logger = init_logger()


def logger_callback(event: EventMsg):
    state = State(os.environ.get("UUID"))

    user_log_format = get_user_request_log_configuration(state.user_command)['log_format']
    if user_log_format == "json":
        msg = msg_to_json(event)
    else:
        msg = event.info.msg.replace('\n', '  ')

    user_log_level = get_user_request_log_configuration(state.user_command)['log_level']
    match event.info.level:
        case "debug":
            logger.debug(msg)
            if user_log_level == "debug":
                state.run_logs.log(event.info.level.upper(), msg)
        case "info":
            logger.info(msg)
            if user_log_level in ["debug", "info"]:
                state.run_logs.log(event.info.level.upper(), msg)
        case "warn":
            logger.warn(msg)
            if user_log_level in ["debug", "info", "warn"]:
                state.run_logs.log(event.info.level.upper(), msg)
        case "error":
            logger.error(msg)
            state.run_logs.log(event.info.level.upper(), msg)


def run_deps(manifest_json):
    logger.info("Running 'dbt deps'")

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

    cli_args = ['deps']

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not (res.success):
        state.run_status = "failed"
        handle_exception(res)


def run_job(manifest_json, state: State, dbt_command: str):

    state.run_status = "running"

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    manifest.build_flat_graph()
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

    # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    cli_args = dbt_command.split(' ')
    logger.info(f"cli args: {cli_args}")

    res_dbt: dbtRunnerResult = dbt.invoke(cli_args)
    if "run" in dbt_command:
        for res in res_dbt.result:
            if str(res.status) != "success":
                if "on-run-end failed" in res.message:
                    logger.info('on-run-end failed')
                else:
                    logger.info('dbt command failed', res)
                    state.run_status = "failed"
                    handle_exception(res)
            else:
                logger.info('dbt success')
    else:
        if not (res_dbt.success):
            state.run_status = "failed"
            handle_exception(res_dbt)

    results = res_dbt.result
    state.run_status = "success"
    return results


def handle_exception(res: dbtRunnerResult):
    logger.error({"error": res})
    if res.exception is not None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


def parse_manifest_from_json(manifest_json):
    partial_parse = msgpack.packb(manifest_json)
    return Manifest.from_msgpack(partial_parse)


def get_user_request_log_configuration(user_command: str):
    command_args_list = split_arg_string(user_command)
    command_context = cli.make_context(info_name='', args=command_args_list)
    command_params = command_context.params
    return {"log_format": command_params['log_format'], "log_level": command_params['log_level']}


if __name__ == '__main__':

    logger.info("Job started")

    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    dbt_command = os.environ.get("DBT_COMMAND")
    request_uuid = os.environ.get("UUID")
    elementary = os.environ.get("ELEMENTARY")

    logger.info("DBT_COMMAND: "+dbt_command)

    state = State(request_uuid)

    # we load manifest.json and dbt_project.yml locally for dbt
    state.get_context_to_local()

    # we extract the manifest
    with open('manifest.json', 'r') as f:
        manifest = json.loads(f.read())

    # check potential dependencies, ex: elementary
    packages_path = './packages.yml'
    check_file = os.path.isfile(packages_path)
    if check_file:
        with open('packages.yml', 'r') as f:
            packages_str = f.read()
        if packages_str != '':
            run_deps(manifest)

    results = run_job(manifest, state, dbt_command)

    # generate elementary report
    if elementary == 'True':
        log = "Generating report..."
        logger.info(log)
        state.run_logs.log("INFO", log)
        report()

    log = "END JOB"
    logger.info(log)
    state.run_logs.log("INFO", log)
