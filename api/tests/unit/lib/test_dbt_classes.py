import pytest
from api.lib.dbt_classes import DbtCommand, FollowUpLink


def test_dbt_command():
    dbt_command = DbtCommand(
        server_url="http://localhost:8001",
        user_command="run",
        manifest="manifest.json",
        dbt_project="my_project",
        seeds={"my_seed": "seed.sql"},
        packages="packages.yml",
        elementary=True
    )

    assert dbt_command.server_url == "http://localhost:8001"
    assert dbt_command.user_command == "run"
    assert dbt_command.processed_command == ""
    assert dbt_command.manifest == "manifest.json"
    assert dbt_command.dbt_project == "my_project"
    assert dbt_command.seeds == {"my_seed": "seed.sql"}
    assert dbt_command.packages == "packages.yml"
    assert dbt_command.elementary == True

def test_follow_up_link():
    follow_up_link = FollowUpLink(
        action_name="my_action",
        link="http://localhost:8001/my_action"
    )

    assert follow_up_link.action_name == "my_action"
    assert follow_up_link.link == "http://localhost:8001/my_action"
