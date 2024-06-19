import concurrent.futures
import json
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


async def unpack_and_persist_artifact(artifact_file: tempfile.SpooledTemporaryFile, destination: Path):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        local_artifact_path = await unpack_artifact(
            artifact_file,
            temp_dir_path
        )
        destination_folder = move_folder(local_artifact_path, destination, delete_after_copy=True)
    return destination_folder


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


def move_folder(
        source_folder: Path,
        destination_folder: Path,
        delete_after_copy: bool = False,
        max_workers: int = 64,
        deadline=None,
        raise_exception: bool = False
) -> Path:
    files = [item for item in source_folder.glob("**/*") if item.is_file()]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for file in files:
            futures.append(executor.submit(
                move_file,
                file,
                destination_folder / file.relative_to(source_folder),
                delete_after_copy
            ))
        concurrent.futures.wait(
            futures, timeout=deadline, return_when=concurrent.futures.ALL_COMPLETED
        )

    results = []
    for future in futures:
        exp = future.exception()

        # If raise_exception is False, don't call future.result()
        if exp and not raise_exception:
            results.append(exp)
        # Get the real result. If there was an exception not handled above,
        # this will raise it.
        else:
            results.append(future.result())
    return destination_folder


def move_file(source: Path, destination: Path, delete_after_copy: bool = False):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if delete_after_copy:
        shutil.move(source, destination)
    else:
        shutil.copy(source, destination)


def persist_metadata(dbt_runtime_config: dict, server_runtime_config: dict, metadata_file: Path) -> Path:
    metadata = {"dbt_runtime_config": dbt_runtime_config, "server_runtime_config": server_runtime_config}
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w") as file:
        json.dump(metadata, file)
    return metadata_file


def load_metadata(metadata_file: Path) -> dict[str, Any]:
    with open(metadata_file, "r") as file:
        return json.load(file)
