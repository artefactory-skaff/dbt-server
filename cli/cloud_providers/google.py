from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import List
from uuid import uuid4

from google.cloud import compute_v1, bigquery, run_v2, storage, iam_admin_v1, resourcemanager_v3
from google.iam.v1 import iam_policy_pb2, policy_pb2
from google.cloud.iam_admin_v1 import types
from google.api_core.exceptions import AlreadyExists, DeadlineExceeded
from googleapiclient import discovery, errors

from cli.server import DbtServer


def deploy(image: str, service_name: str, project_id: str = None):
    if project_id is None:
        project_id = get_project_id()
    enable_gcp_services(["run", "storage", "iam", "bigquery"], project_id)
    bucket = get_or_create_backend_bucket()
    service_account = create_dbt_server_service_account()
    deploy_cloud_run(
        image=image,
        service_name=service_name,
        service_account_email=service_account.email
    )


def get_or_create_backend_bucket(location: str = "eu") -> storage.Bucket:
    project_id = get_project_id()
    bucket_name = f"{project_id}-dbt-server"
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        print(f"Bucket {bucket_name} not found in project {project_id}")
        print(f"Creating bucket {bucket_name}...")
        bucket = storage_client.create_bucket(
            bucket_or_name=bucket_name,
            location=location,
            project=project_id,
        )
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket.patch()
        print(f"Bucket {bucket_name} created in project {project_id}")
    return bucket


def create_dbt_server_service_account() -> iam_admin_v1.ServiceAccount:
    project_id = get_project_id()
    account_id = "dbt-server"
    display_name = "DBT Server Service Account"

    print(f"Creating service account {account_id} in project {project_id}")

    iam_admin_client = iam_admin_v1.IAMClient()
    request = types.CreateServiceAccountRequest()

    request.account_id = account_id
    request.name = f"projects/{project_id}"

    service_account = types.ServiceAccount()
    service_account.display_name = display_name
    request.service_account = service_account

    try:
        account = iam_admin_client.create_service_account(request=request)
    except AlreadyExists:
        print(f"Service account {account_id}@{project_id}.iam.gserviceaccount.com already exists")
        get_request = types.GetServiceAccountRequest(
            name=f"projects/{project_id}/serviceAccounts/{account_id}@{project_id}.iam.gserviceaccount.com"
        )
        account = iam_admin_client.get_service_account(request=get_request)


    roles = [
        "roles/storage.admin",  # TODO: only grant to the dbt-server bucket
        "roles/bigquery.dataEditor",
        "roles/bigquery.jobUser",
        "roles/bigquery.dataViewer",
        "roles/bigquery.metadataViewer",
        "roles/run.developer",
        "roles/logging.logWriter",
        "roles/logging.viewer",
        "roles/cloudscheduler.admin"
    ]

    print(f"Granting roles to {account_id}@{project_id}.iam.gserviceaccount.com:")
    for role in roles:
        print(f"    - {role}")
    modify_policy_add_member(project_id, roles, f"serviceAccount:{account.email}")

    return account


def deploy_cloud_run(image: str, service_name: str, region: str = "europe-west1", service_account_email: str = None):
    project_id = get_project_id()

    print(f"Deploying Cloud Run service {service_name} in project {project_id} with image {image}")

    client = run_v2.ServicesClient()
    parent = f"projects/{project_id}/locations/{region}"

    container = run_v2.Container(
        image=image,
        ports=[run_v2.ContainerPort(container_port=8080)]
    )

    template = run_v2.RevisionTemplate(
        revision=f"{service_name}-{str(uuid4()).split('-')[0]}",
        containers=[container],
        service_account=service_account_email
    )

    service = run_v2.Service(
        template=template,
    )

    request = run_v2.CreateServiceRequest(
        parent=parent,
        service=service,
        service_id=service_name
    )

    service.name = f"projects/{project_id}/locations/{region}/services/{service_name}"
    update_request = run_v2.UpdateServiceRequest(service=service, allow_missing=True)
    try:
        operation = client.update_service(request=update_request)
        operation.result()
        print(f"Service {service_name} updated successfully.")
    except Exception as e:
        print(f"Failed to deploy service {service_name}: {e}")


    return request


def find_dbt_server(location: str = None, gcp_project: str = None) -> str:
    if gcp_project is None:
        gcp_project = get_project_id()

    print(f"Looking for dbt server in project {gcp_project}")

    if location is None:
        print("--gcp-location not set, scanning all GCP regions")
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
        raise Exception(f"No dbt server found on {gcp_project}")
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
    try:
        result = list(run_v2.ServicesClient().list_services(request=request, timeout=10))
    except DeadlineExceeded:
        print(f"Failed to scan potential dbt servers in {region}")
        return []
    return result


@lru_cache(maxsize=128)
def list_regions(project_id):
    regions_client = compute_v1.RegionsClient()
    return [region.name for region in regions_client.list(project=project_id)]

@lru_cache(maxsize=128)
def get_project_id():
    client = bigquery.Client()
    return client.project


def get_project_policy() -> policy_pb2.Policy:
    project_id = get_project_id()
    client = resourcemanager_v3.ProjectsClient()
    request = iam_policy_pb2.GetIamPolicyRequest()
    request.resource = f"projects/{project_id}"

    policy = client.get_iam_policy(request)
    return policy


def modify_policy_add_member(
    project_id: str, roles: List[str], member: str
) -> policy_pb2.Policy:
    policy = get_project_policy()

    roles_in_policy = {bind.role for bind in policy.bindings}

    for role in roles:
        if role not in roles_in_policy:
            new_bind = policy.bindings.add()
            new_bind.role = role
            new_bind.members.append(member)
        else:
            for bind in policy.bindings:
                if bind.role == role:
                    bind.members.append(member)
                    break

    return set_project_policy(project_id, policy)


def set_project_policy(
    project_id: str, policy: policy_pb2.Policy, merge: bool = True
) -> policy_pb2.Policy:
    client = resourcemanager_v3.ProjectsClient()

    request = iam_policy_pb2.GetIamPolicyRequest()
    request.resource = f"projects/{project_id}"
    current_policy = client.get_iam_policy(request)

    policy.ClearField("etag")
    if merge:
        current_policy.MergeFrom(policy)
    else:
        current_policy.CopyFrom(policy)

    request = iam_policy_pb2.SetIamPolicyRequest()
    request.resource = f"projects/{project_id}"

    request.policy.CopyFrom(current_policy)

    policy = client.set_iam_policy(request)
    return policy


def enable_gcp_services(services: List[str], project_id: str):
    service_usage_client = discovery.build("serviceusage", "v1")
    for service in services:
        service_name = f"projects/{project_id}/services/{service}.googleapis.com"
        request = service_usage_client.services().enable(name=service_name)
        try:
            response = request.execute()
            print(f"Enabled service: {service}")
        except errors.HttpError as e:
            if e.resp.status == 409:
                print(f"Service {service} is already enabled.")
            else:
                print(f"Failed to enable service {service}: {e}")
