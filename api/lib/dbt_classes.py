from pydantic import BaseModel


class DbtCommand(BaseModel):
    user_command: str
    processed_command: str = ''
    manifest: str
    dbt_project: str
    packages: str = None
    elementary: bool = False
