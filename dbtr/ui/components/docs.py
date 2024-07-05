import streamlit as st
import streamlit.components.v1 as components

from dbtr.ui.job_with_status import JobWithStatusManager

def docs():
    server = st.session_state["server"]
    latest_runs = JobWithStatusManager(server=server).list(limit=1)
    if len(latest_runs.dbt_remote_jobs) == 0:
        st.write("No runs found")
    else:
        latest_run = latest_runs.dbt_remote_jobs[0]
        st.write(f"Viewing dbt documentation for project {st.session_state['project'].name} as of run {latest_run.run_id} ({latest_run.end_time_humanized})")
        components.iframe(f"{server.server_url}api/run/{latest_run.run_id}/docs/index.html", height=800)
