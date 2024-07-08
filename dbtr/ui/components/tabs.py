import streamlit as st
import extra_streamlit_components as stx


from dbtr.ui.components.run_history import run_history
from dbtr.ui.components.run_logs import run_logs
from dbtr.ui.components.schedules import schedules
from dbtr.ui.components.docs import docs
from dbtr.ui.components.elementary import elementary


def tabs():
    tabs_options = [
        stx.TabBarItemData(id="Runs", title="Runs", description=""),
        stx.TabBarItemData(id="Schedules", title="Schedules", description=""),
        stx.TabBarItemData(id="Docs", title="Docs", description=""),
        stx.TabBarItemData(id="Elementary", title="Elementary", description=""),
    ]
    selected_tab = stx.tab_bar(data=tabs_options, default="Runs")
    if selected_tab == "Runs":
        run_tab()
    elif selected_tab == "Schedules":
        schedules()
    elif selected_tab == "Docs":
        docs()
    elif selected_tab == "Elementary":
        elementary()

def run_tab():
    if "run_id" not in st.session_state:
        run_history()
    else:
        run_logs()
