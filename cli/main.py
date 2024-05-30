import functools
import sys

import click
from dbt.cli.main import cli as dbt_cli
from dbt.cli import requires as dbt_requires

from cli import requires
import cli.params as p


def global_flags(func):
    @p.server_url
    @p.location
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


@remote.command("debug", help="Execute a debug command on a remote server")
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
def debug(ctx, **kwargs):
    server = ctx.obj["server"]
    response = server.send_task(
        ctx.obj["dbt_remote_artifacts"],
        ctx.obj["dbt_runtime_config"],
        ctx.obj["server_runtime_config"]
    )
    click.echo(response)


@remote.command("build", help="Execute a build command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def build(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("run", help="Execute a run command on a remote server")
@click.pass_context
@global_flags
@p.dbt_flags
@p.schedule
@p.schedule_name
def run(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("test", help="Execute a test command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def test(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("run-operation", help="Execute a run-operation command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def run_operation(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("seed", help="Execute a seed command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def seed(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("snapshot", help="Execute a snapshot command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def snapshot(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("source", help="Execute a source command on a remote server")
@click.pass_context
@global_flags
@p.schedule
@p.schedule_name
def source(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("retry", help="Execute a retry command on a remote server")
@click.pass_context
@global_flags
def retry(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


@remote.command("list", help="Execute a list command on a remote server")
@click.pass_context
@global_flags
def list(ctx, **kwargs):
    print(f"Running {sys.argv} on a remote server")


dbt_cli.add_command(remote)

if __name__ == "__main__":
    # https://excalidraw.com/#room=192c05e161acf35b741e,k_uFNcOPJ5urWYd5oVj4vw
    dbt_cli()
