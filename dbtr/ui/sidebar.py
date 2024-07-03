import datetime
import json
import humanize

import streamlit as st
from dbtr.cli.remote_server import DbtServer

server = DbtServer(server_url="http://0.0.0.0:8080")

def show_run(run_id):
    st.session_state["run_id"] = run_id

def extract_timestamp(snowflake_id):
    epoch = 0
    timestamp = (snowflake_id >> 22) + epoch
    return datetime.datetime.fromtimestamp(timestamp / 1000.0)


def sidebar():
    runs = server.session.get(server.server_url + "/api/run")
    runs = runs.json()

    run_details = []
    for run_id, run_info in runs.items():
        command = run_info["dbt_runtime_config"]["command"]
        requester = json.loads(run_info["server_runtime_config"])["requester"]
        run_details.append({"run_id": run_id, "command": command, "requester": requester, "timestamp": extract_timestamp(int(run_id))})

    grouped_runs = {}
    for run in run_details:
        humanized_time = humanize.naturaltime(run["timestamp"])
        if humanized_time not in grouped_runs:
            grouped_runs[humanized_time] = []
        grouped_runs[humanized_time].append(run)

    st.sidebar.title("Runs history")
    with st.sidebar:
        for humanized_time, run_details in grouped_runs.items():
            st.markdown(f"<span style='color: 666;'>{humanized_time}</span>", unsafe_allow_html=True)
            for run in run_details:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(
                        f"""
                        <div style="background-color: #333; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); border-radius: 0.5rem; padding: 0.25rem; margin-bottom: 0.25rem; margin-top: 0.25rem; height:100%">
                            <div>
                                <p style="color: #a0aec0; margin: 0; margin-left: 0.25rem;">{run['timestamp'].strftime("%Y-%m-%d %H:%M:%S")}</p>
                            </div>
                            <div>
                                <p style="font-size: 1.125rem; font-weight: 300; margin: 0; margin-left: 0.25rem;">{run['requester']}</p>
                            </div>
                            <div>
                                <p style="color: #4a5568; margin: 0; margin-left: 0.25rem;">Command: {" ".join(run['command'])}</p>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                with col2:
                    st.markdown("")
                    st.markdown("")  # TODO: find a better way to align the buttons
                    st.button("GO", key=run["timestamp"].strftime("%Y-%m-%d %H:%M:%S"), on_click=show_run, args=(run["run_id"],))
