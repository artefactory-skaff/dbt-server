import functools

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires
from skaff_telemetry import skaff_telemetry

from dbtr.cli import requires
import dbtr.cli.params as p
from dbtr.cli.remote_server import DbtServer, ServerLocked
from dbtr.cli.utils import rename
from dbtr.cli.version import __version__


def global_flags(func):
    @p.server_url
    @p.cloud_provider
    @p.gcp_location
    @p.gcp_project
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
        from dbtr.cli.cloud_providers.google import deploy
        deploy(image=ctx.params["image"], service_name=ctx.params["service"], port=ctx.params["port"], project_id=ctx.params["gcp_project"], log_level=ctx.params["log_level"], adapter=ctx.params["adapter"])
    elif cloud_provider == "local":
        from dbtr.cli.cloud_providers.local import deploy
        deploy(port=ctx.params["port"], log_level=ctx.params["log_level"], adapter=ctx.params["adapter"])
    else:
        click.echo(f"Deploying a dbt server on '{cloud_provider}' is not supported. The only supported providers at the moment are 'google' and 'local'")


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
    @p.schedule
    @p.schedule_name
    @dbt_requires.preflight
    @dbt_requires.profile
    @dbt_requires.project
    @requires.artifacts_archive
    @requires.runtime_config
    @requires.dbt_server
    @rename(name)
    def command_function(ctx, **kwargs):
        @skaff_telemetry(accelerator_name="dbtr-cli", function_name=name, version_number=__version__, project_name=ctx.obj["project"].project_name)
        def inner_command_function(ctx, **kwargs):
            server: DbtServer = ctx.obj["server"]
            response = server.send_task(
                ctx.obj["dbt_remote_artifacts"],
                ctx.obj["dbt_runtime_config"],
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


for name, help_message in commands:
    create_command(name, help_message)

dbt_cli.add_command(remote)


def main():
    try:
        dbt_cli()
    except ServerLocked as e:
        click.echo(f"Run already in progress:\n{e}")
        click.echo("You can unlock the server by running 'dbtr remote unlock'")
        exit(1)
    except Exception as e:
        click.echo(f"Unhandled exception occured: {e}")
        raise


if __name__ == "__main__":
    main()
