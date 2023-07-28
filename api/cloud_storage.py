from google.cloud import storage
from functools import cache


@cache
def connect_client():
    storage_client = storage.Client()
    return storage_client


def write_to_bucket(bucket_name, document_name, data):
    storage_client = connect_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(document_name)

    with blob.open("w") as f:
        f.write(data)


def get_document_from_bucket(bucket_name, document_name):
    storage_client = connect_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(document_name)
    return blob


def get_all_documents_from_folder(bucket_name, folder_name):
    storage_client = connect_client()
    documents = {}
    for blob in storage_client.list_blobs(bucket_name, prefix=folder_name):
        file_name = blob.name.split('/')[-1]
        documents[file_name] = blob.download_as_string(client=None)
    return documents
