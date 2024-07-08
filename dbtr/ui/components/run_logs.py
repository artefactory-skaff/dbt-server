import streamlit as st
import json
from datetime import datetime
import humanize
from ansi2html import Ansi2HTMLConverter

def run_logs():
    run_id = st.session_state.get("run_id", None)
    server = st.session_state["server"]

    col1, col2 = st.columns([1, 7])
    with col1:
        st.write("")
        st.button("Back to runs history", on_click=lambda: st.session_state.pop("run_id"), type="primary")
    with col2:
        st.header(f"Logs for run {run_id}")

    st.markdown(
        """
        <style>
        .ansi30 { color: #000000; }
        .ansi31 { color: #ff0000; }
        .ansi32 { color: #00ff00; }
        .ansi33 { color: #ffff00; }
        .ansi34 { color: #0000ff; }
        .ansi35 { color: #ff00ff; }
        .ansi36 { color: #00ffff; }
        .ansi37 { color: #ffffff; }
        .ansi39 { color: inherit; }
        </style>
        """,
        unsafe_allow_html=True
    )

    response = server.session.get(f"{server.server_url}/api/logs/{run_id}?include_debug=true")
    long_text = response.text.splitlines()

    conv = Ansi2HTMLConverter()

    log_levels = ["ERROR", "WARN", "INFO", "DEBUG"]
    col1, col2, col3 = st.columns([1, 2, 1])  # Adjust the column widths as needed

    with col1:
        success = any(json.loads(line).get("data", {}).get("command_success", False) for line in long_text)
        status = "Success" if success else "Fail"
        status_color = "#55aa55" if success else "#ffcccb"
        with st.container(border=True, height=100):
            st.markdown(
                f"""
                <div style="display: flex; align-items: center; justify-content: center; height: 100%;">
                    <span style="font-size: 24px; color: {status_color};">
                        {"&#x2705;" if success else "&#x274C;"} {status}
                    </span>
                </div>
                """,
                unsafe_allow_html=True
            )


    with col2:
        timestamps = [json.loads(line).get("info", {}).get('ts', '') for line in long_text]
        timestamps = [datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ') for ts in timestamps if ts]
        if timestamps:
            start_time = min(timestamps)
            end_time = max(timestamps)
            duration = humanize.naturaldelta(end_time - start_time)
            with st.container(border=True, height=100):
                st.markdown(
                    f"""
                    <div style="display: flex; align-items: center; justify-content: center; height: 100%;">
                        <span style="font-size: 24px;">This run took {duration}.</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    with col3:
        with st.container(border=True, height=100):
            selected_log_level = st.selectbox("Logs severity", log_levels, index=2)


    with st.container(height=500):
        for line in long_text:
            line_data = json.loads(line)
            info = line_data.get("info", {})
            date_time = info.get("ts", "")
            time_only = datetime.strptime(date_time, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone().strftime('%H:%M:%S') if date_time else ''
            log_level = info.get('level', '').upper()
            if log_levels.index(log_level.upper()) <= log_levels.index(selected_log_level):
                level_color = {
                    "INFO": "#1f77b4",
                    "WARN": "#ff7f0e",
                    "ERROR": "#d62728",
                    "DEBUG": "#808080"
                }.get(log_level.upper(), "#808080")
                msg = conv.convert(info.get('msg', ''), full=False)
                st.markdown(
                    f"""
                    <div style="background-color: #333; padding: 1px; border-radius: 5px; margin-bottom: 1px; color: #fff;">
                        <span style="background-color: #505050; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-size: 14px;">{time_only}</span>
                        <span style="background-color: {level_color}; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-size: 14px;">{log_level}</span>
                        <span style="color: #b0b0b0; font-family: monospace; font-size: 14px; margin-left: 3px;">{msg}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
