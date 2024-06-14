import os
import shlex
from typing import List

from click.testing import CliRunner
import pytest

from cli.main import dbt_cli

SERVER_URL = os.environ.get("SERVER_URL")

@pytest.mark.parametrize("command, expected_in_output", [
    (
        f"dbtr remote debug --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "All checks passed!",
        ]
    ),
    (
        f"dbtr remote build --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=46 WARN=0 ERROR=0 SKIP=0 TOTAL=46",
        ]
    ),
    (
        f"dbtr remote run --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13",
        ]
    ),
    (
        f"dbtr remote test --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=30 WARN=0 ERROR=0 SKIP=0 TOTAL=30",
        ]
    ),
    (
        f"dbtr remote seed --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=0 WARN=0 ERROR=0 SKIP=0 TOTAL=0",
        ]
    ),
    (
        f"dbtr remote snapshot --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "Nothing to do. Try checking your model configs and model specification args",
        ]
    ),
    (
        f"dbtr remote run-operation clean_stale_models --server-url {SERVER_URL} --project-dir jaffle-shop --cloud-provider local",
        [
            "dbt could not find a macro with the name 'clean_stale_models' in any package",
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

def run_command(command: str):
    command_as_args = shlex.split(command.replace("dbtr", "").replace("=", " ").strip())
    return CliRunner().invoke(dbt_cli, command_as_args)
