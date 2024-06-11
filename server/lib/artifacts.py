import tempfile
import zipfile
from pathlib import Path

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
