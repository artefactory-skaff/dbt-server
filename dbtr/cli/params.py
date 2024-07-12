from typing import Any, Callable, Union
import click
from dbt.cli.main import cli as dbt_cli

from dbtr.common.exceptions import MissingAzureParams



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


def register_as_cloud_provider_config(ctx, param, value):
    provider_config = ctx.params.get('provider_config', {})
    if value:
        provider_config[param.name] = value
    ctx.params['provider_config'] = provider_config
    return value


server_url = click.option(
    '--server-url',
    envvar='DBT_SERVER_URL',
    help='Give dbt server url (ex: https://server.com). If not provided, dbt-remote will look for a dbt server on the GCP project set in your gcloud config.'
)


cloud_provider = click.option(
    '--cloud-provider',
    envvar='CLOUD_PROVIDER',
    required=True,
    prompt='Cloud provider where the dbt server runs',
    type=click.Choice(['google', 'azure', 'local'], case_sensitive=False),
    help='Cloud provider where the dbt server runs.',
)

gcp_project = click.option(
    '--gcp-project',
    envvar='GCP_PROJECT',
    help='GCP project where the dbt server runs. Useful for server auto detection. If none is given, the project in your gcloud config will be used.',
    callback=register_as_cloud_provider_config,
)

gcp_location = click.option(
    '--gcp-location',
    envvar='DBT_SERVER_GCP_LOCATION',
    help='Location where the dbt server runs, ex: us-central1.',
    callback=register_as_cloud_provider_config,
)

cpu = click.option(
    '--cpu',
    envvar='DBT_SERVER_CPU',
    default=1,
    help='Number of CPUs to use for the dbt server.',
    callback=register_as_cloud_provider_config,
)

memory = click.option(
    '--memory',
    envvar='DBT_SERVER_MEMORY',
    default="1Gi",
    help='Amount of memory to use for the dbt server in GB.',
    callback=register_as_cloud_provider_config,
)

azure_resource_group = click.option(
    '--azure-resource-group',
    envvar='DBT_SERVER_AZURE_RESOURCE_GROUP',
    help='Resource group where the dbt server runs.',
    callback=register_as_cloud_provider_config,
)

azure_location = click.option(
    '--azure-location',
    envvar='DBT_SERVER_AZURE_LOCATION',
    help='Location where the dbt server runs, ex: francecentral. Useful for server auto detection. If none is given, dbt-remote will look at all EU and US locations.',
    callback=register_as_cloud_provider_config,
)

schedule_cron = click.option(
    '--schedule-cron',
    help='Cron expression to schedule a run. Ex: "0 0 * * *" to run every day at midnight. See https://crontab.guru/ for more information. Warning: Jobs will be scheduled on UTC time.'
)

schedule_name = click.option(
    '--schedule-name',
    help='Name of the cloud scheduler job. If none is given, dbt-remote-<uuid> will be used'
)

schedule_description = click.option(
    '--schedule-description',
    help='Description for the scheduler created. If none is given, "" will be used',
)

schedule_file = click.option(
    '--schedule-file',
    help='Path to a file containing the schedules to deploy. The file should be a yaml, toml or json file.'
)

service = click.option(
    "--service",
    envvar="SERVICE",
    help="Service name for the dbt server."
)

image = click.option(
    "--image",
    envvar="IMAGE",
    default="europe-docker.pkg.dev/dbt-server-sbx-01-caed/dbt-server/prod:latest",
    help="Docker image name to use for the dbt server. Default: europe-docker.pkg.dev/dbt-server-sbx-01-caed/dbt_server/prod:latest"
)

adapter = click.option(
    "--adapter",
    envvar="ADAPTER",
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

auto_approve = click.option(
    "--auto-approve",
    is_flag=True,
    default=False,
    help="Auto approve the command without asking for confirmation."
)

dry_run = click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Dry run the command without invoking it. Will return the context instead of running the command. Useful for debugging."
)

auto_approve = click.option(
    "--auto-approve",
    is_flag=True,
    default=False,
    help="Auto approve the command without asking for confirmation."
)
