import os
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import requests

access_key = os.environ.get('AWS_ACCESS_KEY_ID')
secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
session_token = os.environ.get('AWS_SESSION_TOKEN')
service='lambda'
host = os.environ.get('LAMBDA_HOST')
canonical_uri = "/"
region = os.environ.get("AWS_REGION")
server_url = os.environ.get("SERVER_URL")
method = os.environ.get("HTTP_METHOD")
url=f'https://{host}{canonical_uri}'
server_host = server_url.split("/")[2]


def get_auth_headers():
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region
    )

    request = AWSRequest(
        "GET",
        url,
        headers={'Host': host}
    )

    SigV4Auth(session.get_credentials(), service, region).add_auth(request)

    server_request = AWSRequest(
        method,
        server_url,
        headers={'Host': server_host}
    )

    server_request.headers['X-Amz-Date'] = request.headers['X-Amz-Date']
    server_request.headers['X-Amz-Security-Token'] = request.headers['X-Amz-Security-Token']
    server_request.headers['Authorization'] = request.headers['Authorization']

    return server_request


def main():
    try:
        server_request = get_auth_headers()
        response = requests.request(method, server_url, headers=dict(server_request.headers), data={}, timeout=60)
        response.raise_for_status()
        print(f'Response Status: {response.status_code}')
        print(f'Response Body: {response.content.decode("utf-8")}')
    except Exception as e:
        print(f'Error: {e}')


if __name__ == '__main__':
    main()