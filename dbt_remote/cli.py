import click
from click_aliases import ClickAliasedGroup
from cron_descriptor import get_description

from dbt.cli import main as dbt_cli
from dbt.cli.main import global_flags
from dbt.cli import params as dbt_p

from dbt_remote.src.cli_local_config import LocalCliConfig
from dbt_remote.src.dbt_server_detector import detect_dbt_server_uri
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
@p.version
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
@p.schedule
def dbt(ctx, args, **kwargs):
    getattr(dbt_cli, ctx.info_name).make_context(info_name=ctx.info_name, args=list(args))  # Validates user input
    cli_input = CliInput.from_click_context(ctx)

    click.echo(click.style('Config:', blink=True, bold=True))
    for key, value in cli_input.__dict__.items():
        click.echo(f"   {key}: {value}")

    click.echo('\nSending request to server...')

    server = DbtServer(cli_input.server_url)
    command = DbtServerCommand.from_cli_config(cli_input)
    response = server.send_command(command)

    click.echo(click.style(response.message, blink=True, bold=True))

    if response.links is not None and "last_logs" in response.links:
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
def schedules(ctx, **kwargs):
    pass


@schedules.command("list")
@click.pass_context
@p.server_url
@p.location
def schedules_list(ctx, **kwargs):
    server_url = detect_dbt_server_uri(ctx.params["location"]) if ctx.params["server_url"] is None else ctx.params["server_url"]
    server = DbtServer(server_url)
    schedules = server.list_schedules()

    for schedule in schedules:
        click.echo(click.style(schedule['name'], bold=True))
        click.echo(f"   command: {schedule['command']}")
        click.echo(f"   schedule: {schedule['schedule']} ({get_description(schedule['schedule'])}) {schedule['timezone']}")
        click.echo(f"   target: {schedule['target']}\n")


@schedules.command("describe")
@click.pass_context
@click.argument("name", required=True)
@p.server_url
@p.location
def schedule_describe(ctx, **kwargs):
    server_url = detect_dbt_server_uri(ctx.params["location"]) if ctx.params["server_url"] is None else ctx.params["server_url"]
    server = DbtServer(server_url)
    schedules = server.list_schedules()

    for schedule in schedules:
        if ctx.params["name"] in schedule['name']:
            click.echo(click.style(schedule['name'], bold=True))
            click.echo(f"   command: {schedule['command']}")
            click.echo(f"   schedule: {schedule['schedule']} ({get_description(schedule['schedule'])}) {schedule['timezone']}")
            click.echo(f"   target: {schedule['target']}\n")

@schedules.command("delete")
@click.pass_context
@click.argument("name", required=True)
@p.server_url
@p.location
def schedule_delete(ctx, **kwargs):
    server_url = detect_dbt_server_uri(ctx.params["location"]) if ctx.params["server_url"] is None else ctx.params["server_url"]
    server = DbtServer(server_url)
    response = server.delete_schedule(ctx.params["name"])
    click.echo(response)


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
