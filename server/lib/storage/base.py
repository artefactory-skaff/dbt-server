import abc
from pathlib import Path


class StorageBackend:
    @abc.abstractmethod
    def __init__(self, bucket: str, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def persist_directory(self, source_directory: Path, destination_prefix: str):
        raise NotImplementedError

    @abc.abstractmethod
    def copy_directory(self, source_directory: Path, destination_directory: Path):
        raise NotImplementedError

    @abc.abstractmethod
    def download_directory(self, source_directory: Path, destination_directory: Path) -> Path:
        raise NotImplementedError
