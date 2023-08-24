from google.cloud import storage
from google.cloud.storage import Bucket, Client
from functools import cache
from typing import Dict


def write_to_bucket(bucket_name: str, blob_name: str, data: str) -> ():
    storage_client = connect_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(data)


def get_blob_from_bucket(bucket_name: str, blob_name: str, start_byte: int = 0) -> bytes:
    storage_client = connect_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob_size = get_blob_size(bucket, blob_name)

    if blob_size is not None and blob_size > start_byte:
        blob = blob.download_as_bytes(client=None, start=start_byte)
        return blob
    else:
        return b''


def get_all_blobs_from_folder(bucket_name: str, folder_name: str) -> Dict[str, bytes]:
    storage_client = connect_client()
    blobs = {}
    for blob in storage_client.list_blobs(bucket_name, prefix=folder_name):
        file_name = blob.name.split('/')[-1]
        blobs[file_name] = blob.download_as_bytes(client=None)
    return blobs


def get_blob_size(bucket: Bucket, blob_name: str) -> int:
    blob = bucket.get_blob(blob_name)
    return blob.size


@cache
def connect_client() -> Client:
    storage_client = storage.Client()
    return storage_client
