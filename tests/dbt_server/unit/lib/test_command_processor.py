from unittest.mock import Mock, patch

import pytest
from click import Command, Context
from dbt_server.lib.command_processor import (
    get_arg_list_from_param,
    get_args_list_from_params_dict,
    get_command_args_list,
    get_key_differences_between_dict,
    get_sub_command_args_list,
    get_sub_command_click_context,
    get_sub_command_default_params,
    get_sub_command_name,
    override_command_params,
    override_sub_command_params,
    param_name_to_key_arg_name,
    process_command,
)


def test_process_command():
    command = "list"
    processed_command = process_command(command)
    assert (
        processed_command
        == "--debug --log-format json --log-level debug list --profiles-dir . --project-dir ."
    )


def test_get_sub_command_name():
    mock_context = Mock()
    mock_context.command.name = "freshness"
    assert get_sub_command_name(mock_context) == "source freshness"


def test_get_command_args_list():
    args_list = [
        "--log-format",
        "text",
        "--log-level",
        "debug",
        "run",
        "--select",
        "vbak_dbt",
    ]
    sub_command_args = get_command_args_list(args_list)
    assert isinstance(sub_command_args, list)


def test_override_command_params():
    params = {"log_format": "text", "log_level": "info", "debug": False, "quiet": True}
    assert override_command_params(params) == {
        "log_format": "json",
        "log_level": "debug",
        "debug": True,
        "quiet": False,
    }


def test_get_sub_command_click_context():
    args_list = [
        "--wrong-flag",
        "test",
        "--log-format",
        "text",
        "--log-level",
        "debug",
        "run",
        "--select",
        "vbak_dbt",
    ]
    with pytest.raises(Exception):
        get_sub_command_click_context(args_list)


def test_get_sub_command_args_list():
    args_list = [
        "--log-format",
        "text",
        "--log-level",
        "debug",
        "run",
        "--select",
        "vbak_dbt",
    ]
    context = Mock(spec=Context)
    context.command = Command(name="test_command", callback=lambda: None)
    assert isinstance(get_sub_command_args_list(args_list, context), list)


def test_override_sub_command_params():
    args = {"profiles_dir": "/path/to/profiles", "project_dir": "/path/to/project"}
    assert override_sub_command_params(args) == {
        "profiles_dir": ".",
        "project_dir": ".",
    }


def test_get_sub_command_default_params():
    command = Command(name="test_command", callback=lambda: None)
    default_params = get_sub_command_default_params(command)
    assert isinstance(default_params, dict)


def test_get_args_list_from_params_dict():
    command_key_diff = ["profiles_dir"]
    command_params_dict = {"profiles_dir": ".", "other_key": "value"}
    assert get_args_list_from_params_dict(command_key_diff, command_params_dict) == [
        "--profiles-dir",
        ".",
    ]


def test_get_key_differences_between_dict():
    default_dict = {"key1": "value1", "key2": "value2"}
    new_dict = {"key1": "value1", "key2": "value3", "key3": "value3"}
    assert get_key_differences_between_dict(default_dict, new_dict) == ["key2", "key3"]


def test_get_arg_list_from_param():
    assert get_arg_list_from_param("select", ("model1", "model2")) == [
        "--select",
        "model1",
        "model2",
    ]
    assert get_arg_list_from_param("profiles_dir", ".") == ["--profiles-dir", "."]
    assert get_arg_list_from_param("debug", False) == ["--no-debug"]
    assert get_arg_list_from_param("vars", {"key": "val"}) == ["--vars", "'{key: val}'"]


def test_param_name_to_key_arg_name():
    assert param_name_to_key_arg_name("profiles_dir") == "--profiles-dir"
