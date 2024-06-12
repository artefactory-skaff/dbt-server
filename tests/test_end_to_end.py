import shlex
from time import sleep
from typing import List

from click.testing import CliRunner
import pytest

from cli.main import dbt_cli

@pytest.mark.parametrize("command, expected_in_output", [
    (
        "dbtr remote debug --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "All checks passed!",
        ]
    ),
    (
        "dbtr remote build --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=46 WARN=0 ERROR=0 SKIP=0 TOTAL=46",
        ]
    ),
    (
        "dbtr remote run --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13",
        ]
    ),
    (
        "dbtr remote test --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=30 WARN=0 ERROR=0 SKIP=0 TOTAL=30",
        ]
    ),
    (
        "dbtr remote seed --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "Done. PASS=0 WARN=0 ERROR=0 SKIP=0 TOTAL=0",
        ]
    ),
    (
        "dbtr remote snapshot --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            "Nothing to do. Try checking your model configs and model specification args",
        ]
    ),
    (
        "dbtr remote list --server-url http://0.0.0.0:8080 --project-dir jaffle-shop --cloud-provider local",
        [
            """metric:jaffle_shop.average_order_value
metric:jaffle_shop.count_lifetime_orders
metric:jaffle_shop.cumulative_revenue
metric:jaffle_shop.drink_orders""",
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
