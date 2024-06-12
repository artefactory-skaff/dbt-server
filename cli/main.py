import functools

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires

from cli import requires
import cli.params as p
from cli.remote_server import DbtServer
from cli.utils import rename


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
@global_flags
def remote(ctx, **kwargs):
    pass


@remote.command("deploy", help="Deploy a dbt server on the selected cloud provider")
@click.pass_context
@p.image
@p.service
@p.port
@global_flags
def deploy(ctx, **kwargs):
    cloud_provider = ctx.params["cloud_provider"]
    if cloud_provider == "google":
        from cli.cloud_providers.google import deploy
        deploy(image=ctx.params["image"], service_name=ctx.params["service"], port=ctx.params["port"], project_id=ctx.params["gcp_project"])
    elif cloud_provider == "local":
        from cli.cloud_providers.local import deploy
        deploy(port=ctx.params["port"])
    else:
        click.echo(f"Deploying a dbt server on '{cloud_provider}' is not supported. The only supported providers at the moment are 'google' and 'local'")


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
        server: DbtServer = ctx.obj["server"]
        response = server.send_task(
            ctx.obj["dbt_remote_artifacts"],
            ctx.obj["dbt_runtime_config"],
            ctx.obj["server_runtime_config"]
        )
        for log in response:
            click.echo(log)


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
    ("list", "Execute a list command on a remote server"),
]


for name, help_message in commands:
    create_command(name, help_message)

dbt_cli.add_command(remote)


if __name__ == "__main__":
    dbt_cli()
