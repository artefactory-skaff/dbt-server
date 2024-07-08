import streamlit as st
import streamlit.components.v1 as components
from dbtr.common.remote_server import DbtServer
from dbtr.ui.job_with_status import JobWithStatusManager
from dbtr.ui.server_utils import start_threaded_proxy_server


def docs():
    server: DbtServer = st.session_state["server"]
    latest_runs = JobWithStatusManager(server=server).list(limit=1, project=st.session_state["project"].name)
    if len(latest_runs.dbt_remote_jobs) == 0:
        st.write("No runs found")
    else:
        latest_run = latest_runs.dbt_remote_jobs[0]
        st.write(f"Viewing dbt documentation for project {st.session_state['project'].name} as of run {latest_run.run_id} ({latest_run.end_time_humanized})")
        proxy_url = start_threaded_proxy_server()  # Using a local proxy to handle authentication as the iframe does not support it directly
        components.iframe(f"{proxy_url}/api/run/{latest_run.run_id}/docs/index.html", height=800)
