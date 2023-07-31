import requests
from fastapi import HTTPException
METADATA_URL = 'http://metadata.google.internal/'
HEADERS = {"Metadata-Flavor": "Google"}


def send_metadata_request(path: str):
    url = METADATA_URL + path
    headers = HEADERS
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Metadata not found")
    return res.text


def get_project_id():
    path = "computeMetadata/v1/project/project-id"
    res = send_metadata_request(path)
    return res


def get_location():
    path = "computeMetadata/v1/instance/region"
    res = send_metadata_request(path)
    # output: projects/956787288/regions/us-central1
    location = res.split('/')[-1]
    return location


def get_service_account():
    path = "computeMetadata/v1/instance/service-accounts/default/email"
    res = send_metadata_request(path)
    # output: default/\nstc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com/\n
    return res
