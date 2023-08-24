from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from typing import Dict

from command_processor import process_command, get_sub_command_args_list, get_command_args_list
from command_processor import new_value_to_arg_list


test_dictionnary = {
    "list": {
        "processed_command": "--debug --log-format json list --profiles-dir .",
        "get_command_args_list": ["--debug", "--log-format", "json"],
        "get_sub_command_args_list": ["--profiles-dir", "."]
    },
    "--debug --log-format json list --profiles-dir .": {
        "processed_command": "--debug --log-format json list --profiles-dir .",
        "get_command_args_list": ["--debug", "--log-format", "json"],
        "get_sub_command_args_list": ["--profiles-dir", "."]
    },
    "--no-print --log-format text run --select model": {
        "processed_command": "--no-print --log-format json --debug run --select model --profiles-dir .",
        "get_command_args_list": ['--no-print', '--log-format', 'json', '--debug'],
        "get_sub_command_args_list": ["--select", "model", "--profiles-dir", "."]
    },
    "--log-level error run --select model1 model2": {
        "processed_command": "--log-level debug --debug --log-format json run --select model1 model2 --profiles-dir .",
        "get_command_args_list": ['--log-level', 'debug', "--debug", "--log-format", "json"],
        "get_sub_command_args_list": ["--select", "model1", "model2", "--profiles-dir", "."]
    },
    "--log-level warn --fail-fast --no-debug test": {
        "processed_command": "--log-level debug --fail-fast --debug --log-format json test --profiles-dir .",
        "get_command_args_list": ['--log-level', 'debug', "--fail-fast", "--debug", "--log-format", "json"],
        "get_sub_command_args_list": ["--profiles-dir", "."]
    },
    "test --vars '{"+"key1: val1}'": {
        "processed_command": "--debug --log-format json test --vars '{"+"key1: val1}' --profiles-dir .",
        "get_command_args_list": ["--debug", "--log-format", "json"],
        "get_sub_command_args_list": ["--vars", "'{"+"key1: val1}'", "--profiles-dir", "."]
    },

}


def test_process_command():

    for key in test_dictionnary.keys():
        computed_val = process_command(key)
        expected_val = test_dictionnary[key]["processed_command"]
        assert computed_val == expected_val


def test_get_command_args_list():

    for key in test_dictionnary.keys():

        command = key
        command_args_list = split_arg_string(command)
        computed_val = get_command_args_list(command_args_list)
        expected_val = test_dictionnary[key]["get_command_args_list"]
        assert computed_val == expected_val


def test_get_sub_command_args_list():

    for key in test_dictionnary.keys():

        command = key
        command_args_list = split_arg_string(command)
        command_click_context = args_to_context(command_args_list)
        computed_val = get_sub_command_args_list(command, command_click_context)

        expected_val = test_dictionnary[key]["get_sub_command_args_list"]
        assert computed_val == expected_val


def test_new_value_to_arg_list():
    test_dict = {
        ('flag', True): ['--flag'],
        ('flag', False): ['--no-flag'],
        ('flag_underscore', False): ['--no-flag-underscore'],
        ('flag', ('val',)): ['--flag', 'val'],
        ('flag', ('val1', 'val2')): ['--flag', 'val1', 'val2'],
        ('flag', 'val'): ['--flag', 'val'],
    }

    for key_tuple in test_dict.keys():
        expected_val = test_dict[key_tuple]
        key: str = key_tuple[0]
        val: Dict[str, str] = key_tuple[1]
        computed_val = new_value_to_arg_list(key, val)
        assert expected_val == computed_val

    key_val_dict = {"key": "val"}
    assert new_value_to_arg_list('flag', key_val_dict) == ['--flag', "'{" + "key: val}'"]
