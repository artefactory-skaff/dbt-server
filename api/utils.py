import json
import msgpack
from dbt.contracts.graph.manifest import Manifest

from click.parser import split_arg_string
from dbt.cli.main import cli


def parse_manifest_from_json(manifest_json):
    partial_parse = msgpack.packb(manifest_json)
    return Manifest.from_msgpack(partial_parse)


def parse_manifest_from_payload(manifest_payload: str):
    manifest_str = json.loads(manifest_payload)
    partial_parse = msgpack.packb(manifest_str)
    return Manifest.from_msgpack(partial_parse)


def get_user_request_log_configuration(user_command: str):
    command_args_list = split_arg_string(user_command)
    command_context = cli.make_context(info_name='', args=command_args_list)
    command_params = command_context.params
    return {"log_format": command_params['log_format'], "log_level": command_params['log_level']}
