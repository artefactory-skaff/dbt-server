from google.cloud import storage
from google.cloud.storage import Bucket
from functools import cache
from typing import Dict


@cache
def connect_client():
    storage_client = storage.Client()
    return storage_client


def write_to_bucket(bucket_name: str, document_name: str, data: str) -> ():
    storage_client = connect_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(document_name)

    with blob.open("w") as f:
        f.write(data)


def get_document_from_bucket(bucket_name: str, document_name: str, start_byte: int = 0) -> bytes:
    storage_client = connect_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(document_name)
    blob_size = get_blob_size(bucket, document_name)

    if blob_size is not None and blob_size > start_byte:
        document = blob.download_as_bytes(client=None, start=start_byte)
        return document
    else:
        return b''


def get_blob_size(bucket: Bucket, document_name: str) -> int:
    blob = bucket.get_blob(document_name)
    return blob.size


def get_all_documents_from_folder(bucket_name: str, folder_name: str) -> Dict[str, bytes]:
    storage_client = connect_client()
    documents = {}
    for blob in storage_client.list_blobs(bucket_name, prefix=folder_name):
        file_name = blob.name.split('/')[-1]
        documents[file_name] = blob.download_as_bytes(client=None)
    return documents
