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

cloud_provider = click.option(
    '--cloud-provider',
    envvar='CLOUD_PROVIDER',
    required=True,
    prompt='Cloud provider where the dbt server runs (google, local)',
    help='Cloud provider where the dbt server runs.',
)

gcp_project = click.option(
    '--gcp-project',
    envvar='GCP_PROJECT',
    help='GCP project where the dbt server runs. Useful for server auto detection. If none is given, the project in your gcloud config will be used.'
)

gcp_location = click.option(
    '--gcp-location',
    envvar='DBT_SERVER_GCP_LOCATION',
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

service = click.option(
    "--service",
    envvar="SERVICE",
    default="dbt-server",
    help="Cloud Run service name for the dbt server."
)

image = click.option(
    "--image",
    envvar="IMAGE",
    default="europe-docker.pkg.dev/dbt-server-sbx-f570/dbt-server/prod:latest",
    help="Docker image name to use for the dbt server. Default: europe-docker.pkg.dev/dbt-server-sbx-f570/dbt_server/prod:latest"
)

adapter = click.option(
    "--adapter",
    envvar="ADAPTER",
    required=True,
    prompt="Adapter for the dbt server (dbt-bigquery, dbt-snowflake, ... )",
    help="Adapter to use on the dbt server. dbt-bigquery, dbt-snowflake, etc."
)

port = click.option(
    "--port",
    envvar="PORT",
    default=8080,
    help="Port the dbt server will run on. Default: 8080"
)

log_level = click.option(
    "--log-level",
    envvar="LOG_LEVEL",
    default="INFO",
    help="Log level for the dbt server. Default: INFO"
)
