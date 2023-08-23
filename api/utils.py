import json
import msgpack
from dbt.contracts.graph.manifest import Manifest


def parse_manifest_from_json(manifest_json):
    partial_parse = msgpack.packb(manifest_json)
    return Manifest.from_msgpack(partial_parse)


def parse_manifest_from_payload(manifest_payload: str):
    manifest_str = json.loads(manifest_payload)
    partial_parse = msgpack.packb(manifest_str)
    return Manifest.from_msgpack(partial_parse)
