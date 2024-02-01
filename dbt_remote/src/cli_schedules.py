from typing import Dict
import click
import shlex
import yaml

from cron_descriptor import get_description

from dbt_remote.src.cli_input import CliInput
from dbt_remote.src.cli_utils import run_and_echo
from dbt_remote.src.dbt_server import DbtServer
from dbt_remote.src.dbt_server_detector import get_dbt_server


class Schedules:
    def __init__(self, server_url, location):
        self.server_url = server_url
        self.location = location

        self.server : DbtServer = get_dbt_server(server_url, location)

    def set(self, dbt, cli, schedule_file, auto_approve: bool):
        deployed = self.fetch_deployed()
        requested = self.read_schedules_from_file(schedule_file)

        to_add, to_del, to_redeploy = self.determine_actions(deployed, requested)
        self.print_actions(to_add, to_del, to_redeploy)

        if not auto_approve:
            click.confirm("Do you want to continue?", abort=True)

        self.deploy(dbt, cli, to_add, to_del, to_redeploy)

    def list(self):
        schedules = self.server.list_schedules()

        for schedule in schedules.values():
            click.echo(click.style(schedule['name'], bold=True))
            click.echo(f"   command: {schedule['command']}")
            click.echo(f"   schedule: {schedule['schedule']} ({get_description(schedule['schedule'])}) {schedule['timezone']}")
            click.echo(f"   target: {schedule['target']}\n")

    def describe(self, name):
        schedules = self.server.list_schedules()

        for schedule in schedules.values():
            if name == schedule['name']:
                click.echo(click.style(name, bold=True))
                click.echo(f"   command: {schedule['command']}")
                click.echo(f"   schedule: {schedule['schedule']} ({get_description(schedule['schedule'])}) {schedule['timezone']}")
                click.echo(f"   target: {schedule['target']}\n")
                return
        click.echo(f"Found no schedule named '{name}'")

    def delete(self, name):
        response = self.server.delete_schedule(name)
        click.echo(response)


    def deploy(self, dbt, cli, to_add, to_del, to_redeploy):
        for name in to_del:
            response = self.server.delete_schedule(name)
            click.echo(response)

        for name, schedule in {**to_add, **to_redeploy}.items():
            args = shlex.split(schedule["command"])
            args += ["--schedule", schedule["schedule"]] if "--schedule" not in args else []
            args += ["--schedule-name", name] if "--schedule-name" not in args else []

            parent_ctx = cli.make_context(info_name="", args=args)
            ctx = dbt.make_context(info_name=args[0], parent=parent_ctx, args=args[1:] if len(args) > 1 else [])
            ctx.params["server_url"] = self.server_url
            ctx.params["location"] = self.location
            cli_input = CliInput.from_click_context(ctx)

            run_and_echo(cli_input)

    def print_actions(self, to_add, to_del, to_redeploy):
        click.echo(click.style("\nThe following actions will be performed:", blink=True, bold=True))
        for schedule in to_add:
            click.echo(click.style("+", fg="green") + f" Add: {schedule}")
        for schedule in to_del:
            click.echo(click.style("-", fg="red") + f" Delete: {schedule}")
        for schedule in to_redeploy:
            click.echo(click.style("~", fg="yellow") + f" Redeploy: {schedule}")

    def fetch_deployed(self) -> Dict[str, str]:
        schedules = self.server.list_schedules()
        return schedules

    @staticmethod
    def determine_actions(deployed, requested):
        to_redeploy = {name: schedule for name, schedule in requested.items() if name in deployed and deployed[name] != schedule}
        to_add = {name: schedule for name, schedule in requested.items() if name not in deployed}
        to_del = {name: schedule for name, schedule in deployed.items() if name not in requested}
        return to_add, to_del, to_redeploy

    @staticmethod
    def read_schedules_from_file(schedule_file):
        with open(schedule_file, 'r') as file:
            return yaml.safe_load(file)
