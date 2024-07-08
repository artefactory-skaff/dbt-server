import os
import streamlit as st

from dbtr.ui.server_utils import get_server
from dbtr.ui.components.tabs import tabs
from dbtr.ui.components.project_selector import project_selector


st.set_page_config(layout="wide")

st.session_state["server"] = get_server(os.getenv("DBT_REMOTE_URL"))

col1, col2 = st.columns([3, 1])
with col1:
    st.title("DBT Cloud from Wish")
with col2:
    project_selector()
st.write(f"Connected to remote server at {st.session_state['server'].server_url}")
tabs()
