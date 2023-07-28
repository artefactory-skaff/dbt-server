import os
import firebase_admin
from firebase_admin import firestore
from utils import dbt_command
from datetime import date
from cloud_storage import write_to_bucket, get_all_documents_from_folder

BUCKET_NAME = os.getenv('BUCKET_NAME')

app = firebase_admin.initialize_app()
db = firestore.client()
dbt_collection = db.collection("dbt-status")


class State:

    def __init__(self, uuid: str):
        self.uuid = uuid

    def init_state(self):
        init_firestore_status(self.uuid)

    def get_uuid(self):
        return self.uuid

    def get_storage_folder(self):
        cloud_storage_folder = get_firestore_storage_folder(self.uuid)
        return cloud_storage_folder

    def set_status(self, new_status: str):
        set_firestore_status(self.uuid, new_status)

    def get_status(self):
        status = get_firestore_status(self.uuid)
        return status

    def load_context(self, dbt_command: dbt_command):
        cloud_storage_folder = generate_folder_name(self.uuid)
        print('cloud_storage_folder', cloud_storage_folder)
        set_firestore_storage_folder(self.uuid, cloud_storage_folder)
        load_context_files(dbt_command, cloud_storage_folder)

    def get_context_to_local(self):
        cloud_storage_folder = self.get_storage_folder()
        print("load data from folder", cloud_storage_folder)
        blob_context_files = get_all_documents_from_folder(BUCKET_NAME, cloud_storage_folder)
        for filename in blob_context_files.keys():
            f = open(filename, 'wb')
            f.write(blob_context_files[filename])
            f.close()


def init_firestore_status(uuid: str):
    status_ref = dbt_collection.document(uuid)
    status_ref.set({"uuid": uuid, "status": "created", "cloud_storage_folder": ""})


def set_firestore_status(uuid: str, status: str):
    status_ref = dbt_collection.document(uuid)
    status_ref.update({"status": status})


def get_firestore_status(uuid: str):
    status_ref = dbt_collection.document(uuid)
    return status_ref.get().to_dict()["status"]


def generate_folder_name(uuid: str):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = today_str+"-"+uuid
    return cloud_storage_folder


def set_firestore_storage_folder(uuid: str, cloud_storage_folder: str):
    status_ref = dbt_collection.document(uuid)
    status_ref.update({"cloud_storage_folder": cloud_storage_folder})


def get_firestore_storage_folder(uuid: str):
    status_ref = dbt_collection.document(uuid)
    return status_ref.get().to_dict()["cloud_storage_folder"]


def load_context_files(dbt_command: dbt_command, folder: str):
    write_to_bucket(BUCKET_NAME, folder+"/manifest.json", dbt_command.manifest)
    write_to_bucket(BUCKET_NAME, folder+"/dbt_project.yml", dbt_command.dbt_project)
    write_to_bucket(BUCKET_NAME, folder+"/profiles.yml", dbt_command.profiles)
