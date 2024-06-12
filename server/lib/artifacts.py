import json
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from snowflake import SnowflakeGenerator


def generate_id(prefix: str = "") -> str:
    id_generator = SnowflakeGenerator(instance=1)
    id = next(id_generator)
    return f"{prefix}{id}"


async def unpack_artifact(dbt_remote_artifacts: tempfile.SpooledTemporaryFile, destination_folder: Path):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        artifacts_zip_path = temp_dir_path / dbt_remote_artifacts.filename
        with artifacts_zip_path.open("wb") as f:
            contents = await dbt_remote_artifacts.read()
            f.write(contents)
        with zipfile.ZipFile(artifacts_zip_path, "r") as zip_ref:
            zip_ref.extractall(destination_folder)
        artifacts_zip_path.unlink()
    return destination_folder


def move_folder(source_folder: Path, destination_folder: Path) -> Path:
    if not destination_folder.exists():
        destination_folder.mkdir(parents=True, exist_ok=True)
    for file in source_folder.glob("**/*"):
        if file.is_file():
            file_subpath_name = file.relative_to(source_folder)
            target_file = destination_folder / file_subpath_name
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(file, target_file)

    return destination_folder


def persist_metadata(dbt_runtime_config: dict, server_runtime_config: dict, metadata_file: Path) -> Path:
    metadata = {"dbt_runtime_config": dbt_runtime_config, "server_runtime_config": server_runtime_config}
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w") as file:
        json.dump(metadata, file)
    return metadata_file


def load_metadata(metadata_file: Path) -> dict[str, Any]:
    with open(metadata_file, "r") as file:
        return json.load(file)
