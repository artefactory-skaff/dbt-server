import os
from pathlib import Path
import shlex
from typing import List
from uuid import uuid4
from datetime import datetime

from click.testing import CliRunner
from dbt_remote.cli import cli
import pytest
from google.cloud.devtools.cloudbuild_v1 import CloudBuildClient, ListBuildsRequest

os.environ["LOCAL"] = "true"
os.environ["PROJECT_DIR"] = str(Path(__file__).parent / "dbt_project")

os.environ["SERVICE_ACCOUNT"] = f"dbt-server-service-account@{os.environ['PROJECT_ID']}.iam.gserviceaccount.com"
os.environ["BUCKET_NAME"] = f"{os.environ['PROJECT_ID']}-dbt-server"
os.environ["DOCKER_IMAGE"] = f"{os.environ['LOCATION']}-docker.pkg.dev/{os.environ['PROJECT_ID']}/dbt-server-repository/server-image"
os.environ["ARTIFACT_REGISTRY"] = f"{os.environ['LOCATION']}-docker.pkg.dev/{os.environ['PROJECT_ID']}/dbt-server-repository"

os.environ["UUID"] = str(uuid4())

@pytest.mark.parametrize("command, expected_in_output", [
    (
        "dbt-remote run --select model_that_does_not_exist --schedule '1 2 3 4 5' --schedule-name test-schedule-1",
        [
            "Job run --select model_that_does_not_exist scheduled at 1 2 3 4 5 (At 02:01 AM, on day 3 of the month, only on Friday, only in April) with uuid",
        ]
    ),
    (
        "dbt-remote schedules list",
        [
            "test-schedule-1",
            "command: run --select model_that_does_not_exist",
            "schedule: 1 2 3 4 5 (At 02:01 AM, on day 3 of the month, only on Friday, only in April) UTC"
        ]
    ),
    (
        f"dbt-remote schedules set {Path(__file__).parent / 'schedules.yaml'} --auto-approve",
        [
            "Job build --select my_first_dbt_model scheduled at 2 3 4 5 6 (At 03:02 AM, on day 4 of the month, only on Saturday, only in May) with uuid",
            "- Delete: test-schedule-1",
            "+ Add: test-schedule-2",
        ]
    ),
    (
        "dbt-remote schedules describe test-schedule-1",
        [
            "Found no schedule named 'test-schedule-1'",
        ]
    ),
    (
        "dbt-remote schedules delete test-schedule-2",
        [
            "Schedule test-schedule-2 deleted",
        ]
    ),
    (
        "dbt-remote debug",
        [
            "INFO    [dbt] All checks passed!",
        ]
    ),
    (
        "dbt-remote build",
        [
            """INFO    [dbt] 2 of 8 OK loaded seed file test.test_seed ...................................... [INSERT 1""",
            """ERROR    [dbt] 3 of 8 FAIL 1 not_null_my_first_dbt_model_id ................................... [FAIL 1""",
            """INFO    [dbt] 4 of 8 PASS unique_my_first_dbt_model_id ....................................... [PASS in """,
            """ERROR    [dbt] Failure in test not_null_my_first_dbt_model_id (models/example/schema.yml)""",
        ]
    ),
    (
        "dbt-remote --log-format=json run --select model_that_does_not_exist",
        [
            """"level": "warn", "msg": "Nothing to do. Try checking your model configs and model specification args",""",
        ]
    ),
])
def test_dbt_remote(command, expected_in_output: List[str]):
    result = run_command(command)
    print("CLI output")
    print("-----------")
    print(result.output)
    print("-----------")
    print("Expected to find in output")
    print("-----------")
    [print(f"{expected}") for expected in expected_in_output]
    print("-----------")

    for expected in expected_in_output:
        assert expected in result.output


def test_image_submit():
    start_time = datetime.utcnow()
    result = run_command("dbt-remote image submit")
    assert result.exit_code == 0

    client = CloudBuildClient()
    request = ListBuildsRequest(
        parent=f"projects/{os.environ['PROJECT_ID']}/locations/{os.environ['LOCATION']}",
        project_id=os.environ["PROJECT_ID"],
        filter=f"images={os.environ['DOCKER_IMAGE']}"
    )
    response = client.list_builds(request=request)
    latest_build = next(iter(response), None)

    assert latest_build.status.name == "SUCCESS"
    assert str(latest_build.create_time) > str(start_time)


def run_command(command: str):
    command_as_args = shlex.split(command.replace("dbt-remote", "").replace("=", " ").strip())
    return CliRunner().invoke(cli, command_as_args)
