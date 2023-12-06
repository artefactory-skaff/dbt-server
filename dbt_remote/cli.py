from dbt.cli import main as dbt_cli, params as dbt_p
from dbt.cli.main import global_flags
import click
from click_aliases import ClickAliasedGroup

from dbt_remote.src.cli_local_config import LocalCliConfig
from dbt_remote.src.cli_schedules import Schedules
from dbt_remote.src.cli_utils import run_and_echo
from dbt_remote.src.dbt_server_detector import detect_dbt_server_uri
from dbt_remote.src.dbt_server_image import DbtServerImage
from dbt_remote.src.dbt_server import DbtServer
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
@p.version
@dbt_p.log_format
@p.server_url
@p.location
def cli(ctx, **kwargs):
    pass


@cli.command(
    aliases=[
        "build",
        "clean",
        "compile",
        "debug",
        "deps",
        "init",
        "list",
        "parse",
        "run",
        "retry",
        "clone",
        "run-operation",
        "seed",
        "snapshot",
        "test",
        "docs"
    ],
    context_settings = {"ignore_unknown_options": True}
)
@click.pass_context
@global_flags
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@p.manifest
@dbt_p.target
@p.project_dir
@p.dbt_project
@p.profiles_dir
@p.extra_packages
@p.seeds_path
@p.server_url
@p.location
@p.schedule
@p.schedule_name
def dbt(ctx, args, **kwargs):
    getattr(dbt_cli, ctx.info_name).make_context(info_name=ctx.info_name, args=list(args))  # Validates user input
    cli_input = CliInput.from_click_context(ctx)
    run_and_echo(cli_input)

# ------------------ IMAGE -------------------- #

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


# ------------------ SCHEDULES -------------------- #

@cli.group()
@click.pass_context
def schedules(ctx, **kwargs):
    pass

@schedules.command("set")
@click.pass_context
@click.argument("schedule_file", required=True)
@click.option("--auto-approve", is_flag=True, default=False)
@p.server_url
@p.location
def schedules_set(ctx, **kwargs):
    schedules = Schedules(ctx.params["server_url"], ctx.params["location"])
    schedules.set(dbt, cli, ctx.params["schedule_file"], ctx.params["auto_approve"])

@schedules.command("list")
@click.pass_context
@p.server_url
@p.location
def schedules_list(ctx, **kwargs):
    schedules = Schedules(ctx.params["server_url"], ctx.params["location"])
    schedules.list()

@schedules.command("describe")
@click.pass_context
@click.argument("name", required=True)
@p.server_url
@p.location
def schedule_describe(ctx, **kwargs):
    schedules = Schedules(ctx.params["server_url"], ctx.params["location"])
    schedules.describe(ctx.params["name"])

@schedules.command("delete")
@click.pass_context
@click.argument("name", required=True)
@p.server_url
@p.location
def schedule_delete(ctx, **kwargs):
    schedules = Schedules(ctx.params["server_url"], ctx.params["location"])
    schedules.delete(ctx.params["name"])


# ------------------ CONFIG -------------------- #

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


# ------------------ LOGS -------------------- #

@cli.command(
    "logs",
    context_settings={"help_option_names": ["-h", "--help"]},
    no_args_is_help=True,
)
@p.run_id
@p.server_url
@p.location
@click.pass_context
def logs(ctx, **kwargs):
    server_url = detect_dbt_server_uri(ctx.params["location"]) if ctx.params["server_url"] is None else ctx.params["server_url"]
    server = DbtServer(server_url)
    logs = server.get_logs(ctx.params.get('run_id'))
    for log in logs:
        click.echo(log)


if __name__ == '__main__':
    cli()
