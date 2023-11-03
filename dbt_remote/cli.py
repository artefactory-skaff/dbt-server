import click
from click_aliases import ClickAliasedGroup

from dbt.cli import main as dbt_cli
from dbt.cli.main import global_flags
from dbt.cli import params as dbt_p

from dbt_remote.src.cli_local_config import LocalCliConfig
from dbt_remote.src.dbt_server_image import DbtServerImage
from dbt_remote.src.dbt_server import DbtServer, DbtServerCommand
from dbt_remote.src import cli_params as p
from dbt_remote.src.cli_input import CliInput


help_msg = """
Run dbt commands on a dbt server.

Commands:

list, build, run, run-operation, compile, test, seed, snapshot. (dbt regular commands)

config: configure dbt-remote. See `dbt-remote config help` for more information.

image: build and submit dbt-server image to your Artifact Registry. See `dbt-remote image help` for more information.
"""


@click.group(
    cls=ClickAliasedGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@global_flags
@dbt_p.log_format
@p.server_url
@p.location
def cli(ctx, **kwargs):
    """"""


@cli.command(
        aliases=["build", "clean", "compile", "debug", "deps", "init", "list", "parse", "run", "retry", "clone", "run-operation", "seed", "snapshot", "test", "docs"],
        context_settings = {"ignore_unknown_options": True}
)
@click.pass_context
@global_flags
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@p.manifest
@p.project_dir
@p.dbt_project
@p.profiles_dir
@p.extra_packages
@p.seeds_path
@p.server_url
@p.location
def dbt(ctx, args, **kwargs):
    getattr(dbt_cli, ctx.info_name).make_context(info_name=ctx.info_name, args=list(args))  # Validates user input
    cli_input = CliInput.from_click_context(ctx)

    click.echo(click.style('Config:', blink=True, bold=True))
    for key, value in cli_input.__dict__.items():
        click.echo(f"   {key}: {value}")

    click.echo('\nSending request to server. Waiting for job creation...')

    server = DbtServer(cli_input.server_url)
    command = DbtServerCommand.from_cli_config(cli_input)
    response = server.send_command(command)

    click.echo(f"Job created with uuid: {click.style(response.uuid, blink=True, bold=True)}")

    click.echo('Waiting for job execution...')
    logs = server.stream_logs(response.links["last_logs"])
    for log in logs:
        click.echo(log)


@cli.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
def image(ctx, **kwargs):
    """Manage dbt-remote's Docker image."""


@image.command("submit")
@click.pass_context
@p.location
@p.artifact_registry
def image_submit(ctx, **kwargs):
    kwargs["user_command"] = ctx.info_name
    cli_input = CliInput(**{key: value for key, value in kwargs.items() if key in CliInput.__dataclass_fields__.keys()})
    DbtServerImage(cli_input.location, cli_input.artifact_registry).submit()


@cli.group()
@click.pass_context
def config(ctx, **kwargs):
    pass


@config.command("show")
def config_show():
    click.echo(click.style('Local config:', blink=True, bold=True))
    for key, value in LocalCliConfig().config.items():
        click.echo(f"   {key}: {value}")

@config.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    LocalCliConfig().set(key, value)

@config.command("get")
@click.argument("key")
def config_get(key):
    click.echo(LocalCliConfig().get(key))

@config.command("delete")
@click.argument("key")
def config_delete(key):
    LocalCliConfig().delete(key)


if __name__ == '__main__':
    cli()
