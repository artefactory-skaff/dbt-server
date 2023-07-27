import json
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

def load_file(bucket_name, document_name, destination_document_name):
    blob = get_document_from_bucket(bucket_name,document_name)

    data = blob.download_as_string(client=None)
    f = open(destination_document_name,'wb')
    f.write(data)
    f.close()

def read_file_as_json(bucket_name,manifest_name):
    blob = get_document_from_bucket(bucket_name,manifest_name)

    data = json.loads(blob.download_as_string(client=None))
    return data
