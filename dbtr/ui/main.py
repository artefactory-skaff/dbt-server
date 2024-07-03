import streamlit as st

from ui.run_logs import run_logs
from ui.sidebar import sidebar

st.set_page_config(layout="wide")
sidebar()
run_logs()
