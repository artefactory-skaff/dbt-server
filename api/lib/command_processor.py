from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import cli
from click.core import Command, Context
from fastapi import HTTPException

from typing import List, Dict, Any
import traceback


def process_command(command: str) -> str:
    """
        Ouputs the dbt command the job should invoke.
        Example:

        command: 'list'
        output: '--debug --log-format json --log-level debug list --profiles-dir .'

        command: '--no-debug --log-format text --log-level error list'
        output: '--debug --log-format json --log-level debug list --profiles-dir .'

        command: '--log-level error run --select model1 model2'
        output: '--debug --log-format json --log-level debug run --select model1 model2 --profiles-dir .'

        command: "test --vars '{key1: val1}'"
        output: "--debug --log-format json --log-level debug test --vars '{key1: val1}' --profiles-dir ."

    """

    args_list = split_arg_string(command)
    # ex: ['--log-format', 'text', '--log-level', 'debug', 'run', '--select', 'vbak_dbt']

    command_args = get_command_args_list(args_list)

    sub_command_click_context = get_sub_command_click_context(args_list)
    sub_command_args = get_sub_command_args_list(args_list, sub_command_click_context)

    sub_command_name = get_sub_command_name(sub_command_click_context)
    processed_command = ' '.join(command_args + [sub_command_name] + sub_command_args)
    return processed_command


def get_sub_command_name(sub_command_click_context: Context) -> str:
    sub_command_name = sub_command_click_context.command.name

    if sub_command_name == "freshness":
        sub_command_name = "source freshness"

    return sub_command_name


def get_command_args_list(command_args_list: List[str]) -> List[str]:

    default_context = cli.make_context(info_name='', args=[''])
    default_params_dict = default_context.params

    command_context = cli.make_context(info_name='', args=command_args_list)
    command_context.command.parse_args(default_context, command_args_list)
    command_params_dict = override_command_params(command_context.params)

    command_params_key_diff = get_key_differences_between_dict(default_params_dict, command_params_dict)
    command_args = get_args_list_from_params_dict(command_params_key_diff, command_params_dict)
    return command_args


def override_command_params(params: Dict[str, str]) -> Dict[str, str]:
    params['log_format'] = 'json'
    params['log_level'] = 'debug'
    params['debug'] = True
    params['quiet'] = False
    return params


def get_sub_command_click_context(args_list: List[str]) -> Context:
    try:
        sub_command_click_context = args_to_context(args_list)
        return sub_command_click_context
    except Exception:
        traceback_str = traceback.format_exc()
        raise HTTPException(status_code=400, detail="dbt command failed: " + traceback_str)


def get_sub_command_args_list(args_list: List[str], command_click_context: Context) -> List[str]:

    sub_command: Command = command_click_context.command
    default_params_dict = get_sub_command_default_params(sub_command)

    sub_command_context = args_to_context(args_list)
    sub_command_params_dict = sub_command_context.params
    sub_command_params_dict = override_sub_command_params(sub_command_params_dict)

    sub_command_params_key_diff = get_key_differences_between_dict(default_params_dict, sub_command_params_dict)
    sub_command_args = get_args_list_from_params_dict(sub_command_params_key_diff, sub_command_params_dict)
    return sub_command_args


def override_sub_command_params(args: Dict[str, Any]) -> Dict[str, Any]:
    args['profiles_dir'] = "."
    args['project_dir'] = "."
    return args


def get_sub_command_default_params(command: Command) -> Dict[str, Any]:
    default_args_list = [command.name]
    default_context = cli.make_context(info_name=command.name, args=default_args_list)
    command.parse_args(default_context, default_args_list)
    default_params = default_context.params
    return default_params


def get_args_list_from_params_dict(command_key_diff: List[str], command_params_dict: Dict[str, Any]) -> List[str]:
    """
        Example:

        -   command_key_diff: ['profiles_dir']
            command_args_dict: {'profiles_dir': '.', 'other_key': 'value'}
            output: ['--profiles-dir', '.']

        -   command_key_diff: ['select']
            command_args_dict: {'select': ('model1', 'model2'), 'other_key': 'value'}
            output: ['--select', 'model1', 'model2']

    """

    args_list = []
    for param_key in command_key_diff:
        param_value = command_params_dict[param_key]
        args_list += get_arg_list_from_param(param_key, param_value)
    return args_list


def get_key_differences_between_dict(default_dict: Dict[str, Any], new_dict: Dict[str, Any]) -> List[str]:
    diff_key_list = []
    for key in new_dict.keys():
        if (key not in default_dict.keys()) or (key in default_dict.keys() and default_dict[key] != new_dict[key]):
            diff_key_list.append(key)
    return diff_key_list


def get_arg_list_from_param(param, value) -> List[str]:
    """
        Example:
        - param: 'select'       value: '(model,)'       -> ['--select', 'my_model']
        - param: 'profiles_dir' value: '.'              -> ['--profiles_dir', '.']
        - param: 'debug'        value: False            -> ['--no-debug']
        - param: 'vars'         value: {"key": "val"}   -> ['--vars', "'{key: val}'"]

    """
    key_arg = param_name_to_key_arg_name(param)

    match (value):

        case bool():
            if value:
                return [key_arg]
            else:
                no_key_arg = '--no-'+param.replace('_', '-')
                return [no_key_arg]

        case tuple():
            arg_list = [key_arg]
            for el in value:
                arg_list.append(el)
            return arg_list

        case dict():
            dict_value_str = "'"+str(value).replace("'", '')+"'"  # '{key1: val1}'
            return [key_arg, dict_value_str]

        case int():
            return [key_arg, str(value)]

        case str():
            if key_arg == '--macro':
                return [value]
            return [key_arg, value]

        case _:
            return [key_arg, value]


def param_name_to_key_arg_name(param: str) -> str:
    return '--'+param.replace('_', '-')
