import streamlit as st
from dbtr.ui.job_with_status import JobWithStatusManager, JobStatus
import datetime
import humanize

def run_history():
    st.header(f"Runs history for {st.session_state['project'].name}")

    jobs = JobWithStatusManager(server=st.session_state["server"]).list(project=st.session_state["project"].name)

    with st.container(border=False, height=500):
        for run_details in jobs.dbt_remote_jobs:
            timestamp = datetime.datetime.fromtimestamp((int(run_details.run_id) >> 22) / 1000.0)
            humanized_time = humanize.naturaltime(timestamp)
            result_color = {
                JobStatus.SUCCESS: "#38A169",
                JobStatus.FAILED: "#E53E3E",
                JobStatus.RUNNING: "#3182CE",
                JobStatus.INITIALIZING: "#D69E2E",
                JobStatus.SERVER_ERROR: "#E53E3E",
            }[run_details.run_status]

            col1, col2 = st.columns([16, 1])
            with col1:
                st.markdown(
                    f"""
                    <div style="background-color: #26272F; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); border-radius: 0.5rem; padding: 0.5rem">
                        <div>
                            <span style="background-color: #505050; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace; color: #a0aec0;">{timestamp.strftime("%Y-%m-%d %H:%M:%S")} ({humanized_time})</span>
                            <span style="background-color: #1f77b4; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;">{run_details.requester}</span>
                            <span style="background-color: #808080; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;">{" ".join(run_details.dbt_runtime_config['command'])}</span>
                            <span style='padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;'>{run_details.humanized_model_selection}</span>
                            <span style="background-color: {result_color}; padding: 0px 5px; border-radius: 3px; font-family: monospace; float: right;">{run_details.run_status.value.upper()}</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            with col2:
                st.button("LOGS", key=timestamp.strftime("%Y-%m-%d %H:%M:%S"), on_click=show_run, args=(run_details.run_id,))


def show_run(run_id):
    st.session_state["run_id"] = run_id
