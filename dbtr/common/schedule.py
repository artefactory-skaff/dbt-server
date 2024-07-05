from pathlib import Path
import shlex
from typing import Dict, List, Optional
import click
from cron_descriptor import get_description, Options
import json
import rich
import yaml
import toml

from pydantic import BaseModel, computed_field
from rich.table import Table
from rich.console import Console
from crontzconvert import convert
from tzlocal import get_localzone
from dbt.cli.main import cli

from dbtr.common.exceptions import DbtrException
from dbtr.common.remote_server import DbtServer
from dbtr.common.job import DbtRemoteJob


class Schedule(DbtRemoteJob):
    spec: Optional[Dict] = None

    @classmethod
    def from_schedule_file_spec(cls, schedule_name: str, schedule_file_spec: dict):
        args = get_schedule_args_from_dict(schedule_name, schedule_file_spec)
        args += ["--dry-run"]

        result = cli(args, standalone_mode=False)
        raw_schedule = result.obj["server_runtime_config"]
        raw_schedule["spec"] = schedule_file_spec
        return cls(**raw_schedule)

    @computed_field
    def humanized_cron(self) -> str:
        local_tz = get_localzone()
        localized_cron = convert(self.schedule_cron, "UTC", str(local_tz))
        cron_description_options = Options()
        return f"{get_description(localized_cron, cron_description_options)} ({local_tz})"


    def __rich__(self):
        return render_schedules_table([self])

class Schedules(BaseModel):
    schedules: List[Schedule]

    @computed_field
    def schedules_dict(self) -> dict[str, Schedule]:
        return {schedule.schedule_name: schedule for schedule in self.schedules}

    def __rich__(self):
        return render_schedules_table(self.schedules)


class ScheduleManager:
    def __init__(self, server: DbtServer):
        self.server = server

    def list(self) -> Schedules:
        res = self.server.session.get(url=self.server.server_url + "api/schedule")
        return Schedules(schedules=[Schedule(**schedule_dict) for _, schedule_dict in res.json().items()])

    def get(self, schedule_name: str) -> Schedule:
        res = self.server.session.get(url=self.server.server_url + f"api/schedule/{schedule_name}")
        schedule_dict = res.json()
        return Schedule(**schedule_dict)

    def set_from_file(self, file_path: Path, auto_approve: bool = False):
        schedules_data = load_file(file_path)
        schedules = Schedules(schedules=[Schedule.from_schedule_file_spec(schedule_name, schedule_file_spec) for schedule_name, schedule_file_spec in schedules_data.items()])
        self.set(schedules, auto_approve)

    def set(self, schedules: Schedules, auto_approve: bool = False):
        deployed = self.list()

        to_add, to_del, to_redeploy = self.determine_actions(deployed, schedules)
        self.print_actions(to_add, to_del, to_redeploy)

        if not auto_approve:
            click.confirm("Do you want to continue?", abort=True)

        for schedule in to_del.schedules:
            self.delete(schedule.schedule_name)
        for schedule in to_add.schedules:
            self.create(schedule)
        for schedule in to_redeploy.schedules:
            self.delete(schedule.schedule_name)
            self.create(schedule)

    def create(self, schedule: Schedule):
        rich.print(f"Creating schedule {schedule.schedule_name}")
        args = get_schedule_args_from_dict(schedule.schedule_name, schedule.spec)
        result = cli(args, standalone_mode=False)

    def delete(self, schedule_name: str):
        rich.print(f"Deleting schedule {schedule_name}")
        res = self.server.session.delete(url=self.server.server_url + f"api/schedule/{schedule_name}")
        if not res.ok:
            raise DbtrException(f"Failed to delete schedule: {res.status_code} {res.content}")
        rich.print(f"Deleted schedule {schedule_name}")
        return res.json()

    @staticmethod
    def determine_actions(deployed: Schedules, requested: Schedules) -> tuple[Schedules, Schedules, Schedules]:
        to_redeploy = Schedules(schedules=[schedule for schedule in requested.schedules if schedule.schedule_name in deployed.schedules_dict and deployed.schedules_dict[schedule.schedule_name] != schedule])
        to_add = Schedules(schedules=[schedule for schedule in requested.schedules if schedule.schedule_name not in deployed.schedules_dict])
        to_del = Schedules(schedules=[schedule for schedule in deployed.schedules if schedule.schedule_name not in requested.schedules_dict])
        return to_add, to_del, to_redeploy

    def print_actions(self, to_add: Schedules, to_del: Schedules, to_redeploy: Schedules):
        table = Table(show_header=False)
        for schedule in to_add.schedules:
            table.add_row("Deploy", schedule.schedule_name, schedule.schedule_description, " ".join(schedule.dbt_runtime_config["command"]), schedule.schedule_cron, style="green")
        for schedule in to_del.schedules:
            table.add_row("Delete", schedule.schedule_name, schedule.schedule_description, " ".join(schedule.dbt_runtime_config["command"]), schedule.schedule_cron, style="red")
        for schedule in to_redeploy.schedules:
            table.add_row("Redeploy", schedule.schedule_name, schedule.schedule_description, " ".join(schedule.dbt_runtime_config["command"]), schedule.schedule_cron, style="yellow")

        console = Console()
        console.print("The following actions will be performed:", style="bold")
        console.print(table)


def render_schedules_table(schedules: List[Schedule]):
    table = Table(show_header=True, expand=True)
    table.add_column("Name", justify="center")
    table.add_column("Description", justify="center")
    table.add_column("Command", justify="center")
    table.add_column("Schedule", justify="center")
    local_tz = get_localzone()
    for schedule in schedules:
        localized_cron = convert(schedule.schedule_cron, "UTC", str(local_tz))
        cron_description_options = Options()
        cron_description_options.verbose = True
        cron_description_options.use_24hour_time_format = True
        humanized_cron = f"{get_description(localized_cron, cron_description_options)} ({local_tz})"
        table.add_row(schedule.schedule_name, schedule.schedule_description or "None", " ".join(schedule.dbt_runtime_config["command"]), humanized_cron)
    return table


def load_file(file_path: Path):
    if file_path.suffix == ".json":
        with open(file_path, "r") as file:
            return json.load(file) or {}
    elif file_path.suffix == ".yaml" or file_path.suffix == ".yml":
        with open(file_path, "r") as file:
            return yaml.safe_load(file) or {}
    elif file_path.suffix == ".toml":
        with open(file_path, "r") as file:
            return toml.load(file) or {}
    else:
        raise DbtrException(f"Unsupported file extension: {file_path.suffix}")


def get_schedule_args_from_dict(name: str, schedule_data: dict):
    command = schedule_data["command"]
    schedule_cron = schedule_data["schedule_cron"]
    schedule_description = schedule_data.get("description", None)

    args = shlex.split(command)
    args = args[1:] if args[0] == "dbtr" else args
    args += ["--schedule-name", name]
    args += ["--schedule-cron", schedule_cron]
    if schedule_description:
        args += ["--schedule-description", schedule_description]
    return args
