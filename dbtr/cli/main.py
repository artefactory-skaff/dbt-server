import functools
from pathlib import Path

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires
import rich
from skaff_telemetry import skaff_telemetry

from dbtr.cli import requires
from dbtr.common.exceptions import handle_exceptions
import dbtr.cli.params as p
from dbtr.common.remote_server import DbtServer
from dbtr.common.schedule import ScheduleManager
from dbtr.cli.utils import rename
from dbtr.cli.version import __version__


def global_flags(func):
    @p.server_url
    @p.cloud_provider
    @p.gcp_location
    @p.gcp_project
    @p.gcp_cpu
    @p.gcp_memory
    @p.azure_location
    @p.azure_resource_group
    @p.dry_run
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


@click.group("remote", help="Perform dbt commands on a remote server")
@click.pass_context
def remote(ctx, **kwargs):
    pass


@remote.command("deploy", help="Deploy a dbt server on the selected cloud provider")
@click.pass_context
@global_flags
@p.image
@p.adapter
@p.service
@p.port
@p.log_level
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def deploy(ctx, **kwargs):
    cloud_provider = ctx.params["cloud_provider"]
    if cloud_provider == "google":
        from dbtr.cli.cloud_providers.gcp import deploy
        deploy(
            image=ctx.params["image"],
            service_name=ctx.params["service"],
            port=ctx.params["port"],
            region=ctx.params["gcp_location"],
            project_id=ctx.params["gcp_project"],
            log_level=ctx.params["log_level"],
            adapter=ctx.params["adapter"]
        )
    elif cloud_provider == "azure":
        from dbtr.cli.cloud_providers.az import deploy
        deploy(
            image=ctx.params["image"],
            service_name=ctx.params["service"],
            location=ctx.params["azure_location"],
            adpater=ctx.params["adapter"],
            resource_group=ctx.params["azure_resource_group"],
            log_level=ctx.params["log_level"]
        )
    elif cloud_provider == "local":
        from dbtr.cli.cloud_providers.local import deploy
        deploy(
            port=ctx.params["port"],
            log_level=ctx.params["log_level"],
            adapter=ctx.params["adapter"]
        )
    else:
        click.echo(f"Deploying a dbt server on '{cloud_provider}' is not supported. The only supported providers at the moment are 'google' and 'local'")


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
@requires.dbt_server
@skaff_telemetry(accelerator_name="dbtr-cli", version_number=__version__, project_name='')
def unlock(ctx, **kwargs):
    server: DbtServer = ctx.obj["server"]
    if not click.confirm("Removing the lock will cause issue with the run in progress.\nAre you sure you want to unlock the server?", abort=True):
        return
    server.unlock()


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
