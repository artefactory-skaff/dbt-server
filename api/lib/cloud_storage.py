from functools import cache
from typing import Dict

from google.cloud import storage
# from google.cloud.storage import Bucket, Client
from google.api_core import exceptions
from google.api_core.retry import Retry


class CloudStorage:

    def __init__(self, client: storage.Client):
        self.client = client

    def write_to_bucket(self, bucket_name: str, blob_name: str, data: str) -> ():
        storage_client = self.client
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        retry_policy = define_retry_policy()  # handle 429 error with exponential backoff
        blob.upload_from_string(data, num_retries=5, retry=retry_policy)

    def get_blob_from_bucket(self, bucket_name: str, blob_name: str, start_byte: int = 0) -> bytes:
        storage_client = self.client
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob_size = get_blob_size(bucket, blob_name)

        if blob_size is not None and blob_size > start_byte:
            blob = blob.download_as_bytes(client=None, start=start_byte)
            return blob
        else:
            return b''

    def get_all_blobs_from_folder(self, bucket_name: str, folder_name: str) -> Dict[str, bytes]:
        storage_client = self.client
        blobs = {}
        for blob in storage_client.list_blobs(bucket_name, prefix=folder_name):
            file_name = blob.name.split('/')[-1]
            blobs[file_name] = blob.download_as_bytes(client=None)
        return blobs


@cache
def connect_client() -> storage.Client:
    storage_client = storage.Client()
    return storage_client


def get_blob_size(bucket: storage.Bucket, blob_name: str) -> int:
    blob = bucket.get_blob(blob_name)
    return blob.size


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
