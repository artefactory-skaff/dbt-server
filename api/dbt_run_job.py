import os
import msgpack
import json
from typing import TypedDict
import threading
import time

from click.parser import split_arg_string
from dbt.cli.main import dbtRunner, dbtRunnerResult, cli
from dbt.events.base_types import EventMsg
from dbt.events.functions import msg_to_json
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SeedNode
from elementary.monitor.cli import report
from fastapi import HTTPException

from config import Settings
from clients import LOGGER, CLOUD_STORAGE_INSTANCE, METADATA_DOCUMENT
from lib.state import State
from lib.cloud_storage import CloudStorage
from lib.metadata_document import MetadataDocument


settings = Settings()
callback_lock = threading.Lock()


def prepare_and_execute_job(state: State) -> ():
    state.get_context_to_local()

    manifest = get_manifest()
    manifest = override_manifest_with_correct_seed_path(manifest)
    install_dependencies(state, manifest)

    run_dbt_command(state, manifest, settings.dbt_command)

    with callback_lock:
        LOGGER.log("INFO", "Command successfully executed")

    if ELEMENTARY == "True":
        generate_elementary_report()
        upload_elementary_report(state)

    LOGGER.log("INFO", "END JOB")


def install_dependencies(state: State, manifest: Manifest) -> ():
    packages_path = "./packages.yml"
    check_file = os.path.isfile(packages_path)
    if check_file:
        with open("packages.yml", "r") as f:
            packages_str = f.read()
        if packages_str != "":
            run_dbt_command(state, manifest, "deps")


def run_dbt_command(state: State, manifest: Manifest, dbt_command: str) -> ():
    state.run_status = "running"

    manifest.build_flat_graph()
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

    args_list = split_arg_string(dbt_command)
    res_dbt: dbtRunnerResult = dbt.invoke(args_list)

    if res_dbt.success:
        state.run_status = "success"
    else:
        state.run_status = "failed"

        if ELEMENTARY == "True":
            generate_elementary_report()
            upload_elementary_report(state)

        with callback_lock:
            LOGGER.log("INFO", "END JOB")
        handle_exception(res_dbt.exception)


def generate_elementary_report() -> ():
    LOGGER.log("INFO", "Generating elementary report...")

    report_thread = threading.Thread(target=report, name="Report generator")
    report_thread.start()
    i, timeout = 0, 120
    while report_thread.is_alive() and i < timeout:
        time.sleep(1)
        i += 1

    LOGGER.log("INFO", "Report generated!")


def upload_elementary_report(state: State) -> ():
    LOGGER.log("INFO", "Uploading report...")

    cloud_storage_folder = state.cloud_storage_folder

    with open("edr_target/elementary_report.html", "r") as f:
        elementary_report = f.read()

    CLOUD_STORAGE_INSTANCE.write_file(
        settings.bucket_name,
        cloud_storage_folder + "/elementary_report.html",
        elementary_report,
    )


def logger_callback(event: EventMsg):
    log_configuration = get_user_request_log_configuration(STATE.user_command)

    user_log_format = log_configuration["log_format"]
    if user_log_format == "json":
        msg = msg_to_json(event).replace("\n", "  ")
    else:
        msg = event.info.msg.replace("\n", "  ")

    user_log_level = log_configuration["log_level"]
    match event.info.level:
        case "debug":
            if user_log_level == "debug":
                with callback_lock:
                    LOGGER.log(event.info.level.upper(), msg)
            else:
                LOGGER.logger.debug(msg)

        case "info":
            if user_log_level in ["debug", "info"]:
                with callback_lock:
                    LOGGER.log(event.info.level.upper(), msg)
            else:
                LOGGER.logger.info(msg)

        case "warn":
            if user_log_level in ["debug", "info", "warn"]:
                with callback_lock:
                    LOGGER.log(event.info.level.upper(), msg)
            else:
                LOGGER.logger.warn(msg)

        case "error":
            with callback_lock:
                LOGGER.log(event.info.level.upper(), msg)


LogConfiguration = TypedDict("LogConfiguration", {"log_format": str, "log_level": str})


def get_user_request_log_configuration(user_command: str) -> LogConfiguration:
    command_args_list = split_arg_string(user_command)
    command_context = cli.make_context(info_name="", args=command_args_list)
    command_params = command_context.params
    log_format, log_level = command_params["log_format"], command_params["log_level"]
    if command_params["quiet"]:
        log_level = "error"
    return {"log_format": log_format, "log_level": log_level}


def handle_exception(dbt_exception: BaseException | None):
    LOGGER.log("ERROR", {"error": dbt_exception})
    if dbt_exception is not None:
        raise HTTPException(status_code=400, detail=dbt_exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


def get_manifest() -> Manifest:
    with open("manifest.json", "r") as f:
        manifest_json = json.loads(f.read())
    partial_parse = msgpack.packb(manifest_json)
    manifest: Manifest = Manifest.from_msgpack(partial_parse)
    return manifest


def override_manifest_with_correct_seed_path(manifest: Manifest) -> Manifest:
    """
    Seeds' node root_path will be '.' during the Cloud Run Job.
    """
    nodes_list = manifest.nodes.keys()
    for node_name in nodes_list:
        node = manifest.nodes[node_name]
        if isinstance(node, SeedNode):
            node.root_path = "."
    return manifest


if __name__ == "__main__":
    LOGGER.log("INFO", "Job started")
    state = STATE
    prepare_and_execute_job(state)
