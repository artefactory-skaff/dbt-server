import requests
from subprocess import check_output


def get_auth_session() -> requests.Session:

    id_token = auth_token()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {id_token}"})
    return session


def auth_token() -> str:
    id_token_raw = check_output("gcloud auth print-identity-token", shell=True)
    id_token = id_token_raw.decode("utf8").strip()
    return id_token
