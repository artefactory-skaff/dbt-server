import requests
from typing import List
import traceback
import os
from subprocess import check_output

import click
from google.cloud import run_v2
from google.api_core.exceptions import PermissionDenied

from dbt_remote.src.dbt_remote.server_response_classes import DbtResponseCheck
from dbt_remote.src.dbt_remote.authentication import get_auth_session
from dbt_remote.src.dbt_remote.config_command import CliConfig, set


def detect_dbt_server_uri(cli_config: CliConfig, cloud_run_client: run_v2.ServicesClient) -> str:

    project_id = get_project_id()
    location = cli_config.location  # can be None

    cloud_run_services = get_cloud_run_service_list(project_id, location, cloud_run_client)

    for service in cloud_run_services:
        click.echo('Checking Cloud Run service: ' + service.name)
        auth_session = get_auth_session()

        if check_if_server_is_dbt_server(service, auth_session):
            click.echo('Detected Cloud Run `' + click.style(service.name, blink=True, bold=True) + '` as dbt server')
            if click.confirm('Do you want to use this server as dbt-server?'):
                if click.confirm('Do you want to save this server as default dbt-server in config file?'):
                    set([f'server_url={service.uri}'])
                return service.uri

    click.echo(click.style("ERROR", fg="red"))
    raise click.ClickException(f'No dbt server found in Cloud Run for given project_id ({project_id}) and \
location ({location})')


def get_project_id():
    if 'PROJECT_ID' in os.environ:
        return os.environ['PROJECT_ID']
    else:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException('PROJECT_ID environment variable not found. Please run \
`export PROJECT_ID=<your-project-id>`.')


def get_cloud_run_service_list(project_id: str, location: str | None,
                               client: run_v2.ServicesClient) -> List[run_v2.types.service.Service]:

    if location is None:
        service_list = []
        for region in get_gcp_regions():
            service_list.extend(get_cloud_run_service_list_from_location(project_id, region, client))
        return service_list
    else:
        return get_cloud_run_service_list_from_location(project_id, location, client)


def get_gcp_regions() -> List[str]:
    regions = []

    us_regions_str = check_output("echo $(gcloud compute regions list --filter='name ~ ^us*') | sed 's/UP/\\n/g' \
                                  | awk '{print $1}' | awk NR\\>1", shell=True)
    regions += us_regions_str.decode("utf8").strip().split('\n')

    eu_regions_str = check_output("echo $(gcloud compute regions list --filter='name ~ ^europe*') | sed 's/UP/\\n/g' \
                                  | awk '{print $1}' | awk NR\\>1", shell=True)
    regions += eu_regions_str.decode("utf8").strip().split('\n')

    return regions


def get_cloud_run_service_list_from_location(project_id: str, location: str,
                                             client: run_v2.ServicesClient) -> List[run_v2.types.service.Service]:
    click.echo(f"Listing Cloud Run services from location: {location}")

    parent_value = f"projects/{project_id}/locations/{location}"
    request = run_v2.ListServicesRequest(
        parent=parent_value,
    )

    try:
        list_service_pager = client.list_services(request=request)
        service_list = []
        for service in list_service_pager:
            service_list.append(service)
        click.echo(f"{len(service_list)} services found!")
        return service_list
    except PermissionDenied:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException(f'Permission denied on location `{location}` for project `{project_id}`. \
Note that Cloud Run servers can only be hosted on region (ex: us-central1) not multi-region. \
Please check the server location and specify the correct --location argument.\
\n Ex: `--location us-central1` instead of `--location US`')
    except Exception:
        traceback_str = traceback.format_exc()
        click.echo(traceback_str)
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException('An error occured while listing Cloud Run services')


def check_if_server_is_dbt_server(service: run_v2.types.service.Service, auth_session: requests.Session) -> bool:
    url = service.uri + '/check'
    try:
        res = auth_session.get(url)
    except Exception:  # request timeout or max retries
        return False
    if res.status_code == 200:
        try:
            parsed_response = parse_check_server_response(res)
            if 'Running dbt-server on port' in parsed_response.response:
                return True
            else:
                return False
        except Exception:
            return False
    else:
        return False


def parse_check_server_response(res: requests.Response) -> DbtResponseCheck:
    try:
        results = DbtResponseCheck.parse_raw(res.text)
    except Exception:
        traceback_str = traceback.format_exc()
        raise click.ClickException("Error in parse_check_server: " + traceback_str + "\n Original message: " + res.text)

    results.status_code = res.status_code
    return results
