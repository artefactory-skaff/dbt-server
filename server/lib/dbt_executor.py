import logging
import queue
from pathlib import Path
import threading
from typing import Any

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt_common.events.base_types import EventMsg
from dbt_common.events.functions import msg_to_json


# The "deps" command and the manifest generation are not thread-safe
deps_lock = threading.Lock()

class DBTExecutor:
    LOG_CONFIG = {"log_format": "json", "log_level": "none", "log_level_file": "debug"}

    def __init__(
            self,
            dbt_runtime_config,
            artifact_input: Path,
            logger: logging.Logger,
    ):
        self.dbt_runtime_config = dbt_runtime_config
        self.artifact_input = artifact_input
        self.logger = logger

    def execute_command(self, dbt_command: str, log_queue: queue.Queue):
        dbt_runner = dbtRunner()
        command_args = self.__prepare_command_args(self.dbt_runtime_config, self.artifact_input)

        with deps_lock:
            self.logger.info("Building manifest...")
            manifest = self.__generate_manifest(command_args)

        print(f"Executing dbt command {dbt_command} with artifact input {self.artifact_input.as_posix()}")
        dbt_runner = dbtRunner(manifest=manifest, callbacks=[lambda event: self.handle_event_msg(event, log_queue)])
        dbt_result = dbt_runner.invoke([dbt_command], **{**command_args, **self.LOG_CONFIG})

    @staticmethod
    def __prepare_command_args(command_args: dict[str, Any], remote_project_dir: Path) -> dict[str, Any]:
        args = {key: val for key, val in command_args.items() if not key.startswith("deprecated")}
        args.pop("warn_error_options")  # TODO: define how to handle this
        if "project_dir" in command_args:
            args["project_dir"] = remote_project_dir.as_posix()
        if "profiles_dir" in command_args:
            args["profiles_dir"] = remote_project_dir.as_posix()
        if not command_args.get("select", None):
            args["select"] = ()
        if not command_args.get("exclude", None):
            args["exclude"] = ()
        return args

    def __generate_manifest(self, command_args: dict) -> Manifest:
        res: dbtRunnerResult = dbtRunner().invoke(
            ["parse"],
            **{**command_args, **self.LOG_CONFIG}
        )
        if not res.success:
            raise res.exception
        manifest: Manifest = res.result
        manifest.build_flat_graph()
        return manifest

    @staticmethod
    def handle_event_msg(event: EventMsg, msg_queue: queue.Queue):
        if event.info.level != "debug" or event.info.name == "CommandCompleted":
            msg_queue.put(msg_to_json(event))
