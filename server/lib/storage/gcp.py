from pathlib import Path
from typing import Iterator

from google.cloud import storage
from google.cloud.storage import Blob

from server.lib.storage.base import StorageBackend


class GCPStorageBackend(StorageBackend):
    def __init__(self, bucket: str):
        client = storage.Client()
        self.bucket = client.get_bucket(bucket)

    def persist_directory(self, source_directory: Path, destination_prefix: str):
        for file in source_directory.glob("**/*"):
            if file.is_file():
                destination_path = Path(destination_prefix) / file.relative_to(source_directory)
                blob = self.bucket.blob(destination_path.as_posix())
                blob.upload_from_filename(file.as_posix())

    def copy_directory(self, source_directory: Path, destination_directory: Path):
        blobs: Iterator[Blob] = self.bucket.list_blobs(prefix=source_directory.as_posix())
        for blob in blobs:
            destination_blob_name = destination_directory / Path(blob.name).relative_to(source_directory)
            self.bucket.copy_blob(blob, self.bucket, destination_blob_name.as_posix())

    def download_directory(self, source_directory: Path, destination_directory: Path) -> Path:
        blobs: Iterator[Blob] = self.bucket.list_blobs(prefix=source_directory.as_posix())
        for blob in blobs:
            local_blob_name = destination_directory / Path(blob.name).relative_to(source_directory)
            local_blob_name.parent.mkdir(parents=True, exist_ok=True)
            print(local_blob_name)
            blob.download_to_filename(local_blob_name.as_posix())
        return destination_directory
