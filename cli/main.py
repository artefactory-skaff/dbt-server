import functools

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires

from cli import requires
import cli.params as p
from cli.utils import rename


def global_flags(func):
    @p.server_url
    @p.gcp_location
    @p.cloud_provider
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


def create_command(name, help_message):
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
        server = ctx.obj["server"]
        response = server.send_task(
            ctx.obj["dbt_remote_artifacts"],
            ctx.obj["dbt_runtime_config"],
            ctx.obj["server_runtime_config"]
        )
        click.echo(response)


commands = [
    ("debug", "Execute a debug command on a remote server"),
    ("build", "Execute a build command on a remote server"),
    ("run", "Execute a run command on a remote server"),
    ("test", "Execute a test command on a remote server"),
    ("run-operation", "Execute a run-operation command on a remote server"),
    ("seed", "Execute a seed command on a remote server"),
    ("snapshot", "Execute a snapshot command on a remote server"),
    ("source", "Execute a source command on a remote server"),
    ("retry", "Execute a retry command on a remote server"),
    ("list", "Execute a list command on a remote server"),
]


for name, help_message in commands:
    create_command(name, help_message)

dbt_cli.add_command(remote)


if __name__ == "__main__":
    dbt_cli()
