from typing import Any, Callable, Union
import click
from dbt.cli.main import cli as dbt_cli


def dbt_flags(command: Union[click.Command, Callable[..., Any]]):
    """Dynamically add the dbt flags of the original command to its remote version. This makes sure the help and validation of the original command is preserved in the remote version, and should allow us to support a broader range of past and future versions of dbt."""

    command_name = command.name if isinstance(command, click.Command) else command.__name__
    for param in dbt_cli.commands[command_name].params:
        if isinstance(command, click.Command):
            command.params.append(param)
        else:
            if not hasattr(command, "__click_params__"):
                command.__click_params__ = []  # type: ignore
            command.__click_params__.append(param)  # type: ignore
    return command


server_url = click.option(
    '--server-url',
    envvar='DBT_SERVER_URL',
    help='Give dbt server url (ex: https://server.com). If not provided, dbt-remote will look for a dbt server on the GCP project set in your gcloud config.'
)

location = click.option(
    '--location',
    envvar='DBT_SERVER_LOCATION',
    help='Location where the dbt server runs, ex: us-central1. Useful for server auto detection. If none is given, dbt-remote will look at all EU and US locations.'
)

schedule = click.option(
    '--schedule',
    help='Cron expression to schedule a run. Ex: "0 0 * * *" to run every day at midnight. See https://crontab.guru/ for more information.'
)

schedule_name = click.option(
    '--schedule-name',
    help='Name of the cloud scheduler job. If none is given, dbt-remote-<uuid> will be used'
)

artifact_registry = click.option(
    '--artifact-registry',
    envvar='ARTIFACT_REGISTRY',
    required=True,
    prompt=True,
    help='The artifact registry the dbt-server image will be pushed to. Ex: europe-west9-docker.pkg.dev/my-project/my-registry'
)
