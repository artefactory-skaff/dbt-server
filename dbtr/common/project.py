from pydantic import BaseModel, computed_field

from dbtr.common.remote_server import DbtServer


class Project(BaseModel):
    name: str

class Projects(BaseModel):
    projects: list[Project]

    @computed_field
    def projects_dict(self) -> dict[str, Project]:
        return {project.name: project for project in self.projects}


class ProjectManager:
    def __init__(self, server: DbtServer):
        self.server = server

    def list(self) -> Projects:
        res = self.server.session.get(url=self.server.server_url + "api/project")
        return Projects(projects=[Project(**project_dict) for project_dict in res.json().values()])
