import streamlit as st

from dbtr.common.remote_server import DbtServer
from dbtr.cli.cloud_providers.gcp import get_auth_token  # TODO: multicloud support

@st.cache_resource
def get_server(url: str):
    return DbtServer(server_url=url, token_generator=get_auth_token)
