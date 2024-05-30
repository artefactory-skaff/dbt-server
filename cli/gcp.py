from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from google.cloud import compute_v1, bigquery, run_v2

from cli.server import DbtServer


def find_dbt_server(location: str = None, gcp_project: str = None) -> str:
    if gcp_project is None:
        gcp_project = get_project_id()

    print(f"Looking for dbt server in project {gcp_project}")

    if location is None:
        print("No location provided, looking through all GCP regions. Accelerate the search by specifying a --gcp-location")
        regions = list_regions(gcp_project)
    else:
        regions = [location]

    with ThreadPoolExecutor() as executor:
        result = executor.map(list_cloud_run_services, regions, [gcp_project] * len(regions))

    result = [service.uri for sublist in result for service in sublist]

    dbt_servers = []
    for uri in result:
        if DbtServer(uri).is_dbt_server():
            dbt_servers.append(uri)

    if len(dbt_servers) == 0:
        print(f"No dbt server found on {gcp_project}")
    elif len(dbt_servers) > 1:
        print(f"Multiple dbt servers found: {dbt_servers}")
        print(f"Using the first one at {dbt_servers[0]}")
        print(f"You can explicitely set the server with --server-url")
    else:
        print(f"Found dbt server at {dbt_servers[0]}")

    return dbt_servers[0]


def list_cloud_run_services(region: str, project_id: str):
    parent_value = f"projects/{project_id}/locations/{region}"
    request = run_v2.ListServicesRequest(parent=parent_value)
    result = list(run_v2.ServicesClient().list_services(request=request))
    return result


@lru_cache(maxsize=128)
def list_regions(project_id):
    regions_client = compute_v1.RegionsClient()
    return [region.name for region in regions_client.list(project=project_id)]

@lru_cache(maxsize=128)
def get_project_id():
    client = bigquery.Client()
    return client.project
