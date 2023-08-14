import json
import msgpack
from dbt.contracts.graph.manifest import Manifest
from pydantic import BaseModel
from click.parser import split_arg_string
from dbt.cli.flags import Flags, args_to_context
from dbt.cli.main import cli


class dbt_command(BaseModel):
    command: str
    manifest: str
    dbt_project: str


def parse_manifest_from_json(manifest_json):
    partial_parse = msgpack.packb(manifest_json)
    return Manifest.from_msgpack(partial_parse)


def parse_manifest_from_payload(manifest_payload: str):
    manifest_str = json.loads(manifest_payload)
    partial_parse = msgpack.packb(manifest_str)
    return Manifest.from_msgpack(partial_parse)


def context_param_diff(command: str, main_command: str):
    split_args = split_arg_string(command)
    # 'run --select my_model my_other_model'
    # --> ['--select', 'my_model', 'my_other_model']

    # we get the default context for this command (ex: run)
    context = cli.make_context(info_name=main_command, args=split_args)
    context.command.get_command(context, main_command).parse_args(context, split_args)
    default_params = context.params

    # we get the custom context (ex: run --select my_model)
    ctx = args_to_context(split_args)
    new_params = ctx.params

    # we compare both contexts and extract list of changes to give dbt
    # (ex: ['--select', 'my_model'])
    new_list = args_to_list(default_args=default_params, new_args=new_params)
    return new_list


def args_to_list(default_args, new_args):
    """
    default_args, new_args are dictionnaries
    this function identifies which values changed between both dictionnaries
    then converts the changed values into a list
    ex: if profiles-dir value changed, it returns ['--profiles-dir', '.']
    """
    new_list = []
    for key in new_args:
        if key in default_args.keys():
            if default_args[key] != new_args[key]:
                new_value = new_args[key]
                args_to_add = changed_value_to_list(key, new_value)
                new_list = new_list + args_to_add
    return (new_list)


def changed_value_to_list(key, new_value):
    key_flag = '--'+key.replace('_', '-')
    args_to_add = [key_flag]
    match (new_value):
        case tuple():
            for el in new_value:
                args_to_add.append(el)
        case dict():
            args_to_add.append(str(new_value))
        case bool():
            if new_value:
                args_to_add = [key_flag]
            else:
                args_to_add = ['--no-'+key.replace('_', '-')]
        case _:
            args_to_add.append(new_value)
    return args_to_add


def process_command(command: str):

    split_args = split_arg_string(command)
    # '--log-level info --debug run --select my_model my_other_model'
    # --> ['--select', 'my_model', 'my_other_model']

    # get context and flags from command
    ctx = args_to_context(split_args)
    args = Flags(ctx)
    main_command = ctx.command.name
    initial_args = args.__dict__.copy()

    # set right --log-level, --log-format, --profiles-dir, --debug
    args, new_command = set_correct_settings_for_dbt_execution(args, command)

    # identify the args that have been modified, both for the command (--profiles-dir, --select)
    # and for the context (--log-level, --log-format)
    command_args = context_param_diff(new_command, main_command)
    print("command args:", command_args)

    final_args = args.__dict__
    context_args = args_to_list(initial_args, final_args)
    print("context args:", context_args)

    # join the different args in a complete command string
    command_list = context_args + [main_command] + command_args
    processed_command = ' '.join(command_list)

    return processed_command


def set_correct_settings_for_dbt_execution(args: Flags, command: str):
    new_command = command
    if args.log_format != "json":
        object.__setattr__(args, 'log_format', 'json')
    if args.log_level != "debug":
        object.__setattr__(args, 'log_level', 'debug')
    if not (args.debug):
        object.__setattr__(args, 'debug', True)
    if args.profiles_dir != ".":
        new_command += " --profiles-dir ."
    return args, new_command
