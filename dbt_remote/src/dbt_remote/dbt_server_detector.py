from typing import List
import traceback
import os
from subprocess import check_output
import json
from multiprocessing.pool import ThreadPool

import click
from google.cloud import run_v2
import requests


def detect_dbt_server_uri(location: str) -> str:
    project_id = get_project_id()
    location = location  # can be None

    if location is not None:
        click.echo(f"\nLooking for dbt server on project {project_id} in {location}...")
    else:
        click.echo(f"\nLooking for dbt server on project {project_id}...")

    cloud_run_services = get_cloud_run_service_list(project_id, location)

    for service in cloud_run_services:
        if check_if_server_is_dbt_server(service):
            server_url = service.uri if service.uri.endswith('/') else service.uri + "/"
            return server_url

    click.echo(click.style("ERROR", fg="red"))
    raise click.ClickException(f'No dbt server found in GCP project "{project_id}"')


def get_project_id():
    if 'PROJECT_ID' in os.environ:
        return os.environ['PROJECT_ID']
    else:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException('PROJECT_ID environment variable not found. Please run \
`export PROJECT_ID=<your-project-id>`.')


def get_cloud_run_service_list(project_id: str, location: str | None) -> List[run_v2.types.service.Service]:
    regions = get_gcp_regions() if location is None else [location]
    services_matrix = ThreadPool(50).starmap(get_cloud_run_service_list_from_location, [(project_id, region) for region in regions])
    services = [service for services in services_matrix for service in services]
    return services


def get_gcp_regions() -> List[str]:
    regions_raw = check_output("gcloud run regions list --format json", shell=True)
    regions = [region["locationId"] for region in json.loads(regions_raw)]
    return regions


def get_cloud_run_service_list_from_location(project_id: str, location: str) -> List[run_v2.types.service.Service]:

    parent_value = f"projects/{project_id}/locations/{location}"
    request = run_v2.ListServicesRequest(
        parent=parent_value,
    )

    try:
        return list(run_v2.ServicesClient().list_services(request=request))
    except:
        traceback_str = traceback.format_exc()
        click.echo(traceback_str)


def check_if_server_is_dbt_server(service: run_v2.types.service.Service) -> bool:
    url = service.uri + '/check'
    auth_session = get_auth_session()

    try:
        res = auth_session.get(url)
        if "dbt-server" in res.json()["response"]:
            return True
        return False
    except Exception:  # request timeout or max retries
        return False

def get_auth_session() -> requests.Session:
    id_token_raw = check_output("gcloud auth print-identity-token", shell=True)
    id_token = id_token_raw.decode("utf8").strip()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {id_token}"})
    return session