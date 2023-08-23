from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import cli
from click.core import Command, Context

from typing import List, Dict, Any


def process_command(command: str) -> str:
    """
        Example:
        command: '--log-level info --debug run --select my_model my_other_model'
    """

    command_args_list = split_arg_string(command)
    # ex: ['--log-format', 'text', '--log-level', 'debug', 'run', '--select', 'vbak_dbt']

    command_click_context = args_to_context(command_args_list)
    sub_command_args = get_sub_command_args_list(command, command_click_context)
    print("sub_command_args:", sub_command_args)

    command_args = get_command_args_list(command)
    print("command args:", command_args)

    sub_command = command_click_context.command
    processed_command = ' '.join(command_args + [sub_command.name] + sub_command_args)
    return processed_command


def get_command_args_list(command: str) -> List[str]:

    command_args_list = split_arg_string(command)

    default_context = cli.make_context(info_name='', args=[''])
    default_args_dict = default_context.params

    command_context = cli.make_context(info_name='', args=command_args_list)
    command_context.command.parse_args(default_context, command_args_list)
    command_args_dict = override_command_args(command_context.params)

    command_args = get_list_of_differences_between_dict(default_args_dict, command_args_dict)
    return command_args


def override_command_args(args: Dict[str, str]) -> Dict[str, str]:
    if args['log_format'] != "json":
        args['log_format'] = 'json'
    if args['log_level'] not in ["debug", "info"]:
        args['log_level'] = 'debug'
    if not args['debug']:
        args['debug'] = True
    return args


def get_sub_command_args_list(command: str, command_click_context: Context) -> List[str]:
    """
        Example:
        command: "--log-format text --log-level debug run --select vbak_dbt --profiles-dir ."
        sub_command: "run"

    """
    sub_command: Command = command_click_context.command
    split_args = split_arg_string(command)
    # ex: ['--log-format', 'text', '--log-level', 'debug', 'run', '--select', 'vbak_dbt', '--profiles-dir', '.']

    default_args = get_default_params_from_sub_command(sub_command)

    sub_command_context = args_to_context(split_args)
    sub_command_args = sub_command_context.params
    sub_command_args = override_sub_command_args(sub_command_args)

    sub_command_args = get_list_of_differences_between_dict(default_args, sub_command_args)
    return sub_command_args


def override_sub_command_args(args: Dict[str, str]) -> Dict[str, str]:
    new_args = args.copy()
    if args['profiles_dir'] != ".":
        new_args['profiles_dir'] = "."
    return new_args


def get_default_params_from_sub_command(command: Command) -> Dict[str, Any]:
    default_split_args = [command.name]
    default_context = cli.make_context(info_name=command.name, args=default_split_args)
    command.parse_args(default_context, default_split_args)
    default_params = default_context.params
    return default_params


def get_list_of_differences_between_dict(default_args: Dict[str, Any], new_args: Dict[str, Any]) -> List[str]:
    """
    default_args, new_args are dictionnaries
    this function identifies new and changed values in new_args (compared to default_args)
    then converts them into a list
    ex: if 'profiles_dir' value changed, it returns ['--profiles-dir', 'new value']
    """
    diff_list = []
    for key in new_args.keys():
        if (key not in default_args.keys()) or (key in default_args.keys() and default_args[key] != new_args[key]):
            new_value = new_args[key]
            args_to_add = new_value_to_arg_list(key, new_value)
            diff_list = diff_list + args_to_add
    return (diff_list)


def new_value_to_arg_list(key, new_value) -> List[str]:
    """
        Example:
        - key: 'select', new_value: '(model,)'  -> ['--select', 'my_model']
        - key: 'profiles_dir', new_value: '.'   -> ['--select', 'my_model']
        - key: 'debug', value: False            -> ['--no-debug']
    """
    key_flag = '--'+key.replace('_', '-')
    args_to_add = [key_flag]
    match (new_value):
        case bool():
            if new_value:
                return [key_flag]
            else:
                return ['--no-'+key.replace('_', '-')]
        case tuple():
            for el in new_value:
                args_to_add.append(el)
        case dict():
            args_to_add.append("'"+str(new_value).replace("'", '')+"'")
        case _:
            args_to_add.append(new_value)
    return args_to_add
