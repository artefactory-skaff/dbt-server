import pytest
from dbt_remote.cli import (
    assemble_dbt_command,
    get_selected_nodes,
    parse_server_response,
    send_command,
)


def test_assemble_dbt_command():
    user_command, args = "list", []
    expected_dbt_command = "list"
    assert assemble_dbt_command(user_command, args) == expected_dbt_command

    user_command, args = "list", ["--select", "mymodel"]
    expected_dbt_command = "list '--select' 'mymodel'"
    assert assemble_dbt_command(user_command, args) == expected_dbt_command

    user_command, args = "list", ["--select", "mymodel", "--var", "{hey: val}"]
    expected_dbt_command = "list '--select' 'mymodel' '--var' '{hey: val}'"
    assert assemble_dbt_command(user_command, args) == expected_dbt_command


def test_send_command(MockSendCommandRequest, PatchBuiltInOpen):
    server_url = "https://test-server.test"

    send_command_list = [
        {
            "command": "command",
            "project_dir": ".",
            "manifest": "manifest",
            "dbt_project": "dbt_project",
            "packages": None,
            "seeds_path": "data",
            "elementary": False,
            "data": {
                "server_url": server_url,
                "user_command": "command",
                "manifest": "data...",
                "dbt_project": "data...",
            },
        },
        {
            "command": "command",
            "project_dir": ".",
            "manifest": "manifest",
            "dbt_project": "dbt_project",
            "packages": "packages",
            "seeds_path": "data",
            "elementary": True,
            "data": {
                "server_url": server_url,
                "user_command": "command",
                "manifest": "data...",
                "dbt_project": "data...",
                "packages": "data...",
                "elementary": True,
            },
        },
        {
            "command": "seed --select my_seed",
            "project_dir": ".",
            "manifest": "manifest",
            "dbt_project": "dbt_project",
            "packages": None,
            "seeds_path": "data",
            "elementary": False,
            "data": {
                "server_url": server_url,
                "user_command": "seed --select my_seed",
                "manifest": "data...",
                "dbt_project": "data...",
                "seeds": {},
            },
        },
    ]

    send_command_requests_mock = MockSendCommandRequest

    for context_dict in send_command_list:
        with PatchBuiltInOpen:
            res = send_command(
                server_url,
                context_dict["command"],
                context_dict["project_dir"],
                context_dict["manifest"],
                context_dict["dbt_project"],
                context_dict["packages"],
                context_dict["seeds_path"],
                context_dict["elementary"],
            )

        assert send_command_requests_mock.last_request.method == "POST"
        assert send_command_requests_mock.last_request.url == f"{server_url}/dbt"
        assert send_command_requests_mock.last_request.json() == context_dict["data"]
        assert res.json() == {"name": "awesome-mock"}


class TestResponseServer:
    def __init__(self, new_text, new_status):
        self._text = new_text
        self._status = new_status

    @property
    def text(self) -> str:
        return self._text

    @property
    def status_code(self) -> str:
        return self._status


def test_parse_server_response():
    response_list = [
        {
            "status_code": 200,
            "str": '{"uuid": "0000", "links": [{"action_name": "action", "link": "link"}]}',
            "expected_parsed_response": {
                "status_code": 200,
                "uuid": "0000",
                "detail": None,
                "links": [{"action_name": "action", "link": "link"}],
            },
        },
        {
            "status_code": 400,
            "str": '{"detail": "detail"}',
            "expected_parsed_response": {
                "status_code": 400,
                "uuid": None,
                "detail": "detail",
                "links": None,
            },
        },
    ]

    for test_case in response_list:
        server_response = parse_server_response(
            TestResponseServer(test_case["str"], test_case["status_code"])
        )
        expected_response = test_case["expected_parsed_response"]

        assert server_response.status_code == expected_response["status_code"]
        assert server_response.uuid == expected_response["uuid"]
        assert server_response.detail == expected_response["detail"]

        assert (server_response.links is None and expected_response["links"] is None) or (
            len(server_response.links) == len(expected_response["links"])
        )
        if server_response.links is not None:
            for i in range(len(server_response.links)):
                expected_link = expected_response["links"][i]
                actual_link = server_response.links[i]
                assert actual_link.action_name == expected_link["action_name"]
                assert actual_link.link == expected_link["link"]


def test_get_selected_nodes():
    project_dir = " --project-dir ."
    commands_dict = {
        "run": {"select": []},
        "run --select mymodel": {"select": ["mymodel"]},
        "run -s mymodel": {"select": ["mymodel"]},
        "run --select mymodel1 mymodel2": {"select": ["mymodel1", "mymodel2"]},
    }

    for command in commands_dict.keys():
        expected_target = commands_dict[command]["select"]
        computed_target = get_selected_nodes(command + project_dir)
        assert computed_target == expected_target
