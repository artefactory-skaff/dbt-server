import os
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import requests


def get_auth_headers(server_url, method):
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    session_token = os.environ.get('AWS_SESSION_TOKEN')
    service='lambda'
    lambda_url = os.environ.get('LAMBDA_URL')
    canonical_uri = "/"
    region = os.environ.get("AWS_REGION")
    server_host = server_url.split("/")[2]
    lambda_host = lambda_url.split("/")[2]

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region
    )
    request = AWSRequest(
        "GET",
        lambda_url,
        headers={'Host': lambda_host}
    )
    SigV4Auth(session.get_credentials(), service, region).add_auth(request)

    request_headers = {'Host': server_host}
    request_headers['X-Amz-Date'] = request.headers['X-Amz-Date']
    request_headers['X-Amz-Security-Token'] = request.headers['X-Amz-Security-Token']
    request_headers['Authorization'] = request.headers['Authorization']

    return request_headers


def main():
    try:
        server_url = os.environ.get("SERVER_URL")
        method = os.environ.get("HTTP_METHOD")

        server_request_headers = get_auth_headers(server_url, method)
        response = requests.request(method, server_url, headers=dict(server_request_headers), data={}, timeout=60)
        response.raise_for_status()

        print(f'Response Status: {response.status_code}')
        print(f'Response Body: {response.content.decode("utf-8")}')
    except Exception as e:
        print(f'Error: {e}')


if __name__ == '__main__':
    main()