import click
from functools import cache
import os
from typing import Dict

import google.auth.transport.requests
import google.oauth2.id_token


def get_auth_headers(run_service: str, creds_path: str) -> Dict[str, str]:

    if creds_path is None:
        click.echo("No credentials file provided. Trying to connect to dbt server without authentication.")
        return {}

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
    id_token = get_id_token(run_service)
    return {'Authorization': f'Bearer {id_token}'}


@cache
def get_id_token(audience: str):
    """
        audience is the service url. ex: https://my-service.a.run.app/
    """

    print('GOOGLE_APPLICATION_CREDENTIALS', os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

    return id_token
