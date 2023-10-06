from typing import Dict

import os
from abc import ABC, abstractmethod

try:
    from google.api_core import exceptions
    from google.api_core.retry import Retry
    from google.cloud import storage
except ImportError:
    storage = None  # type: ignore
    exceptions = None  # type: ignore
    Retry = None  # type: ignore
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None  # type: ignore

from dbt_server.config import Settings

settings = Settings()


class Storage(ABC):
    @abstractmethod
    def write_file(self, bucket_name: str, file_name: str, data: str) -> None:
        pass

    @abstractmethod
    def get_file(self, bucket_name: str, file_name: str, start_byte: int = 0) -> bytes:
        pass

    @abstractmethod
    def get_file_console_url(self, bucket_name: str, file_name: str) -> str:
        pass

    @abstractmethod
    def get_files_in_folder(self, bucket_name: str, folder_name: str) -> Dict[str, bytes]:
        pass


class LocalStorage(Storage):
    def write_file(self, bucket_name: str, file_name: str, data: str) -> None:
        with open(f"{bucket_name}/{file_name}", "w") as file:
            file.write(data)

    def get_file(self, bucket_name: str, file_name: str, start_byte: int = 0) -> bytes:
        with open(f"{bucket_name}/{file_name}", "rb") as file:
            file.seek(start_byte)
            return file.read()

    def get_file_console_url(self, bucket_name: str, file_name: str) -> str:
        return f"{bucket_name}/{file_name}"

    def get_files_in_folder(self, bucket_name: str, folder_name: str) -> Dict[str, bytes]:
        return {
            file: open(file, "rb").read()
            for file in [
                os.path.join(dp, f)
                for dp, dn, filenames in os.walk(f"{bucket_name}/{folder_name}")
                for f in filenames
            ]
        }


class GoogleCloudStorage(Storage):
    def __init__(self):
        self.client = storage.Client()

    def write_file(self, bucket_name: str, file_name: str, data: str) -> None:
        # Implement Google Cloud Storage specific logic here
        blob = self.client.bucket(bucket_name).blob(file_name)
        retry_policy = (
            GoogleCloudStorage.define_retry_policy()
        )  # handle 429 error with exponential backoff
        blob.upload_from_string(data, num_retries=5, retry=retry_policy)

    def get_file(self, bucket_name: str, file_name: str, start_byte: int = 0) -> bytes:
        # Implement Google Cloud Storage specific logic here
        blob = self.client.get_bucket(bucket_name).get_blob(file_name)
        if blob.size is not None and blob.size > start_byte:
            blob_bytes = blob.download_as_bytes(client=None, start=start_byte)
            return blob_bytes
        else:
            return b""

    def get_file_console_url(self, bucket_name: str, file_name: str) -> str:
        blob_client = self.client.bucket(bucket_name).blob(file_name)
        console_url = "https://console.cloud.google.com/storage/browser/_details"
        return f"{console_url}/{bucket_name}/{blob_client.path}"

    def get_files_in_folder(self, bucket_name: str, folder_name: str) -> Dict[str, bytes]:
        # Implement Google Cloud Storage specific logic here
        blobs = {}
        for blob in self.client.list_blobs(bucket_name, prefix=folder_name):
            blobs[blob.name] = blob.download_as_bytes(client=None)
        return blobs

    @staticmethod
    def define_retry_policy():
        _MY_RETRIABLE_TYPES = [
            exceptions.TooManyRequests,  # 429
            exceptions.InternalServerError,  # 500
            exceptions.BadGateway,  # 502
            exceptions.ServiceUnavailable,  # 503
        ]

        def is_retryable(exc):
            return isinstance(exc, _MY_RETRIABLE_TYPES)

        retry_policy = Retry(predicate=is_retryable)
        retry_policy = retry_policy.with_delay(initial=1.5, multiplier=1.2, maximum=45.0)


class AzureBlobStorage(Storage):
    def __init__(self):
        self.client = BlobServiceClient.from_connection_string(
            settings.azure.blob_storage_connection_string
        )

    def write_file(self, bucket_name: str, file_name: str, data: str) -> None:
        blob_client = self.client.get_blob_client(bucket_name, file_name)
        blob_client.upload_blob(data)

    def get_file(self, bucket_name: str, file_name: str, start_byte: int = 0) -> bytes:
        blob_client = self.client.get_blob_client(bucket_name, file_name)
        download_stream = blob_client.download_blob(offset=start_byte)
        data = download_stream.readall()
        return data if isinstance(data, bytes) else bytes(data, "utf-8")

    def get_file_console_url(self, bucket_name: str, file_name: str) -> str:
        blob_client = self.client.get_blob_client(bucket_name, file_name)
        return blob_client.url

    def get_files_in_folder(self, bucket_name: str, folder_name: str) -> Dict[str, bytes]:
        container_client = self.client.get_container_client(bucket_name)
        blob_list = container_client.list_blobs(name_starts_with=folder_name)
        return {blob.name: self.get_file(bucket_name, blob.name) for blob in blob_list}


class StorageFactory:
    @staticmethod
    def create(service_type):
        if service_type == "GoogleCloudStorage":
            return GoogleCloudStorage()
        elif service_type == "AzureBlobStorage":
            return AzureBlobStorage()
        elif service_type == "LocalStorage":
            return LocalStorage()
        else:
            raise ValueError("Invalid service type")
