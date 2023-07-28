import json
import msgpack
from dbt.contracts.graph.manifest import Manifest
from pydantic import BaseModel


class dbt_command(BaseModel):
    command: str
    args: dict[str, str] = None
    manifest: str
    dbt_project: str
    profiles: str


def parse_manifest_from_json(manifest_json):
    partial_parse = msgpack.packb(manifest_json)
    return Manifest.from_msgpack(partial_parse)


def parse_manifest_from_payload(manifest_payload):
    manifest_str = json.loads(manifest_payload)
    partial_parse = msgpack.packb(manifest_str)
    return Manifest.from_msgpack(partial_parse)


def parse_command(command):
    # command examples: list, run --select vbak
    command_list = command.split(" ")
    if len(command_list) == 0:
        return "", {}
    if len(command_list) == 1:
        return command_list[0], {}
    else:
        main_command = command_list[0]
        args = {}
        i = 1
        arg_key = ""
        while i < len(command_list):
            arg = command_list[i]
            if arg[0] == "-":
                arg_key = arg
                args[arg_key] = ""
            else:
                args[arg_key] = arg
            i += 1
        return main_command, args


def parse_args(arg_dict):
    arg_list = []
    if arg_dict is None or arg_dict == {}:
        return arg_list
    for arg_key in arg_dict.keys():
        arg_list.append(arg_key)
        arg_list.append(arg_dict[arg_key])
    return arg_list
