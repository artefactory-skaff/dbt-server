import abc
from pathlib import Path


class StorageBackend:
    @abc.abstractmethod
    def __init__(self, bucket: str, **kwargs):
        pass

    @abc.abstractmethod
    def persist_directory(self, source_directory: Path, destination_prefix: str):
        pass

    @abc.abstractmethod
    def copy_directory(self, source_directory: Path, destination_directory: Path):
        pass

    @abc.abstractmethod
    def download_directory(self, source_directory: Path, destination_directory: Path) -> Path:
        pass
