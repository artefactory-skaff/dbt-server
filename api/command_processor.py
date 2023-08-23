from click.parser import split_arg_string
from dbt.cli.flags import Flags, args_to_context
from dbt.cli.main import cli
from state import State


def process_command(state: State, command: str) -> str:
    """
        Example:
        command: '--log-level info --debug run --select my_model my_other_model'
    """

    sub_command_args_list = split_arg_string(command)

    sub_command_click_context = args_to_context(sub_command_args_list)
    flags = Flags(sub_command_click_context)
    sub_command_name = sub_command_click_context.command.name
    initial_flags = flags.__dict__.copy()

    flags, command = override_user_flags(flags, command)

    command_args = context_param_diff(command, sub_command_name)
    print("command args:", command_args)

    final_args = flags.__dict__
    context_args = args_to_list(initial_flags, final_args)
    print("context args:", context_args)

    # join the different args in a complete command string
    command_list = context_args + [sub_command_name] + command_args
    processed_command = ' '.join(command_list)

    return processed_command


def override_user_flags(args: Flags, command: str):
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


def context_param_diff(command: str, sub_command: str):
    split_args = split_arg_string(command)
    # '--log-level info run --select my_model my_other_model'
    # --> ['--select', 'my_model', 'my_other_model']

    # we get the default context for this command (ex: run)
    context = cli.make_context(info_name=sub_command, args=split_args)
    context.command.get_command(context, sub_command).parse_args(context, split_args)
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
