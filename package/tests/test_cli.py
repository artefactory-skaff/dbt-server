from src.dbt_remote.cli import (
    assemble_dbt_command,
    parse_server_response,
    get_selected_nodes,
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
    res_dict = {
        '{"uuid": "0000"}': {"uuid": "0000", "detail": None},
        '{"detail": "detail"}': {"uuid": None, "detail": "detail"},
    }
    for server_response in res_dict.keys():
        dbt_response = parse_server_response(TestResponseServer(server_response, 200))
        assert dbt_response.uuid == res_dict[server_response]["uuid"]


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
