import sys
import os

sys.path.insert(1, './lib')

from cloud_storage import write_to_bucket, get_blob_from_bucket, get_all_blobs_from_folder


def test_write_to_bucket_and_get_blob_from_bucket():
    bucket_name = 'dbt-stc-test'
    blob_name = 'test/test'
    data = 'hello world'

    write_to_bucket(bucket_name, blob_name, data)

    blob = get_blob_from_bucket(bucket_name, blob_name, 0)
    assert blob == b'hello world'

    blob = get_blob_from_bucket(bucket_name, blob_name, 6)
    assert blob == b'world'

    blob = get_blob_from_bucket(bucket_name, blob_name, 30)
    assert blob == b''


def test_get_all_blobs_from_folder():
    bucket_name = 'dbt-stc-test'
    blob_name = 'test/test'
    data = 'hello world'
    write_to_bucket(bucket_name, blob_name, data)
    blob_name = 'test/test2'
    data = 'hello world'
    write_to_bucket(bucket_name, blob_name, data)

    blobs_dict = get_all_blobs_from_folder(bucket_name, 'test')

    assert len(blobs_dict.keys()) == 2
    assert blobs_dict['test'] == b'hello world'
    assert blobs_dict['test2'] == b'hello world'
