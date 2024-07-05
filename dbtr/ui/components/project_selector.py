import streamlit as st

from dbtr.common.project import ProjectManager

def project_selector():
    server = st.session_state["server"]
    projects = ProjectManager(server).list()
    with st.container():
        project = st.selectbox("Select a project", projects.projects, format_func=lambda project: project.name)
        st.session_state["project"] = project
