import functools
import os
from pathlib import Path

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires
import rich
from skaff_telemetry import skaff_telemetry
from streamlit.web.cli import main as stcli

from dbtr.cli import requires
from dbtr.common.exceptions import handle_exceptions
import dbtr.cli.params as p
from dbtr.common.remote_server import DbtServer
from dbtr.common.schedule import ScheduleManager
from dbtr.cli.utils import rename
from dbtr.cli.version import __version__


def global_flags(func):
    @p.server_url
    @p.log_level
    @p.dry_run
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group("remote", help="Perform dbt commands on a remote server")
@click.pass_context
def remote(ctx, **kwargs):
    pass


@remote.group("deploy", help="Deploy a dbt server on the selected cloud provider")
@global_flags
@click.pass_context
def deploy(ctx, **kwargs):
    pass

@deploy.command("google", help="Deploy a dbt server on Google Cloud")
@click.pass_context
@global_flags
@p.gcp_project
@p.gcp_location
@p.cpu
@p.memory
@p.adapter
@p.image
@p.service
@p.auto_approve
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def deploy_google(ctx, **kwargs):
    from dbtr.cli.cloud_providers import gcp

    if not ctx.params.get("gcp_project"):
        ctx.params["gcp_project"] = click.prompt("GCP project", default=gcp.get_project_id())
    if not ctx.params.get("gcp_location"):
        ctx.params["gcp_location"] = click.prompt("Server location", default="europe-west1")
    if not ctx.params.get("service"):
        ctx.params["service"] = click.prompt("Name of the Cloud Run service", default="dbt-server")
    if not ctx.params.get("adapter"):
        ctx.params["adapter"] = click.prompt("Adapter for the dbt server", default="dbt-bigquery")

    gcp.deploy(
        image=ctx.params["image"],
        service_name=ctx.params["service"],
        region=ctx.params["gcp_location"],
        project_id=ctx.params["gcp_project"],
        cpu=ctx.params["cpu"],
        memory=ctx.params["memory"],
        log_level=ctx.params["log_level"],
        adapter=ctx.params["adapter"],
        auto_approve=ctx.params["auto_approve"]
    )

@deploy.command("azure", help="Deploy a dbt server on Azure")
@click.pass_context
@global_flags
@p.azure_resource_group
@p.azure_location
@p.adapter
@p.image
@p.service
@p.auto_approve
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def deploy_azure(ctx, **kwargs):
    from dbtr.cli.cloud_providers.az import deploy

    if not ctx.params.get("azure_resource_group"):
        ctx.params["azure_resource_group"] = click.prompt("Azure resource group")
    if not ctx.params.get("azure_location"):
        ctx.params["azure_location"] = click.prompt("Azure location", default="francecentral")
    if not ctx.params.get("service"):
        ctx.params["service"] = click.prompt("Name of the Container App", default="dbt-server")
    if not ctx.params.get("adapter"):
        ctx.params["adapter"] = click.prompt("Adapter for the dbt server (dbt-snowflake, dbt-bigquery, ...)")

    deploy(
        resource_group=ctx.params["azure_resource_group"],
        location=ctx.params["azure_location"],
        service_name=ctx.params["service"],
        image=ctx.params["image"],
        adpater=ctx.params["adapter"],
        log_level=ctx.params["log_level"],
        auto_approve=ctx.params["auto_approve"]
    )

@deploy.command("local", help="Deploy a dbt server locally")
@click.pass_context
@global_flags
@p.port
@p.adapter
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def deploy_local(ctx, **kwargs):
    from dbtr.cli.cloud_providers.local import deploy
    deploy(
        port=ctx.params["port"],
        log_level=ctx.params["log_level"],
        adapter=ctx.params["adapter"]
    )

@remote.group("schedule", help="Manage your scheduled runs")
@click.pass_context
def schedule(ctx, **kwargs):
    pass


@schedule.command("list", help="List all scheduled jobs")
@click.pass_context
@global_flags
@p.cloud_provider
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def list_schedules(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    schedules = ScheduleManager(server).list()
    rich.print(schedules)


@schedule.command("get", help="Get details about a scheduled job")
@click.pass_context
@global_flags
@p.cloud_provider
@p.schedule_name
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def get_schedule(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    schedule = ScheduleManager(server).get(ctx.params["schedule_name"])
    rich.print(schedule)


@schedule.command("set", help="Set a schedule from a file")
@click.pass_context
@global_flags
@p.cloud_provider
@p.schedule_file
@p.auto_approve
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def set_schedules(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    ScheduleManager(server).set_from_file(Path(ctx.params["schedule_file"]), auto_approve=ctx.params["auto_approve"])
    rich.print("Schedules have been set:")
    schedules = ScheduleManager(server).list()
    rich.print(schedules)


@schedule.command("delete", help="Delete a scheduled job")
@click.pass_context
@global_flags
@p.cloud_provider
@p.schedule_name
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def delete_schedules(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    ScheduleManager(server).delete(ctx.params["schedule_name"])
    click.echo(f"Deleted job '{ctx.params['schedule_name']}'")


@remote.command("unlock", help="Forcefully remove a server lock")
@click.pass_context
@global_flags
@p.cloud_provider
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def unlock(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    if not click.confirm("Removing the lock will cause issue with the run in progress.\nAre you sure you want to unlock the server?", abort=True):
        return
    server.unlock()


@remote.command("frontend", help="Open the dbt remote server frontend")
@click.pass_context
@global_flags
@p.cloud_provider
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def frontend(ctx, **kwargs):
    ui_path = Path(__file__).parent.parent / "ui" / "main.py"
    os.environ["DBT_REMOTE_URL"] = ctx.params["server_url"]
    stcli(["run", str(ui_path)])


def create_command(name, help_message):
    """
    Dynamically creates a new command for the remote CLI group.

    Args:
        name (str): The name of the command.
        help_message (str): The help message for the command.
    """
    @remote.command(name, help=help_message)
    @click.pass_context
    @global_flags
    @p.dbt_flags
    @p.cloud_provider
    @p.schedule_cron
    @p.schedule_name
    @p.schedule_description
    @dbt_requires.preflight
    @dbt_requires.profile
    @dbt_requires.project
    @requires.artifacts_archive
    @requires.runtime_config
    @requires.dbt_server
    @rename(name)
    def command_function(ctx, **kwargs):
        if ctx.params["dry_run"]:
            return ctx

        @skaff_telemetry(accelerator_name="dbtr-cli", function_name=name, version_number=__version__, project_name=ctx.obj["project"].project_name)
        def inner_command_function(ctx, **kwargs):
            server: DbtServer = ctx.obj["server"]
            response = server.send_task(
                ctx.obj["dbt_remote_artifacts"],
                ctx.obj["server_runtime_config"]
            )
            for log in response:
                click.echo(log)
        return inner_command_function(ctx, **kwargs)


# Subset of base dbt commands that can be used a subcommands of the `remote` group
commands = [
    ("debug", "Execute a debug command on a remote server"),
    ("build", "Execute a build command on a remote server"),
    ("run", "Execute a run command on a remote server"),
    ("test", "Execute a test command on a remote server"),
    ("run-operation", "Execute a run-operation command on a remote server"),
    ("seed", "Execute a seed command on a remote server"),
    ("snapshot", "Execute a snapshot command on a remote server"),
    ("retry", "Execute a retry command on a remote server"),
]


# Dynamically build the remote commands
for name, help_message in commands:
    create_command(name, help_message)


# Extend the dbt CLI with the remote commands
dbt_cli.add_command(remote)


# Run the CLI and catch top-level exceptions
def main():
    try:
        dbt_cli()
    except Exception as e:
        handle_exceptions(e)


if __name__ == "__main__":
    main()
