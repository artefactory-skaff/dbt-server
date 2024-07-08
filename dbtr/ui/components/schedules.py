import streamlit as st

from dbtr.common.schedule import ScheduleManager


def schedules():
    st.header(f"Scheduled jobs for {st.session_state['project'].name}")

    schedules = ScheduleManager(st.session_state["server"]).list()
    project_schedules = [schedule for schedule in schedules.schedules if schedule.project == st.session_state["project"].name]

    for schedule in project_schedules:
        st.markdown(
            f"""
            <div style="background-color: #26272F; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); border-radius: 0.5rem; padding: 0.5rem; margin-bottom: 0.5rem;">
                <div>
                    <span style="background-color: #505050; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace; color: #a0aec0;">{schedule.schedule_name}</span>
                    <span style="background-color: #1f77b4; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;">{schedule.humanized_cron}</span>
                    <span style="background-color: #808080; padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;">{" ".join(schedule.dbt_runtime_config['command'])}</span>
                    <span style='padding: 2px 5px; border-radius: 3px; margin-left: 3px; font-family: monospace;'>{schedule.humanized_model_selection}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
