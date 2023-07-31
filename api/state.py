import os
from google.cloud import firestore
from utils import dbt_command
from datetime import date
from cloud_storage import write_to_bucket, get_all_documents_from_folder

BUCKET_NAME = os.getenv('BUCKET_NAME')

client = firestore.Client()
dbt_collection = client.collection("dbt-status")


class State:

    def __init__(self, uuid: str):
        self.uuid = uuid

    def init_state(self):
        status_ref = dbt_collection.document(self.uuid)
        status_ref.set({"uuid": self.uuid, "status": "created", "cloud_storage_folder": ""})

    @property
    def uuid(self):
        return self.uuid

    @property
    def status(self):
        status_ref = dbt_collection.document(self.uuid)
        status = status_ref.get().to_dict()["status"]
        return status

    @status.setter
    def status(self, new_status: str):
        status_ref = dbt_collection.document(self.uuid)
        status_ref.update({"status": new_status})

    @property
    def storage_folder(self):
        status_ref = dbt_collection.document(self.uuid)
        cloud_storage_folder = status_ref.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @storage_folder.setter
    def storage_folder(self, cloud_storage_folder: str):
        status_ref = dbt_collection.document(self.uuid)
        status_ref.update({"cloud_storage_folder": cloud_storage_folder})

    def load_context(self, dbt_command: dbt_command):
        cloud_storage_folder = generate_folder_name(self.uuid)
        print('cloud_storage_folder', cloud_storage_folder)
        self.storage_folder = cloud_storage_folder
        load_context_files(dbt_command, cloud_storage_folder)

    def get_context_to_local(self):
        cloud_storage_folder = self.storage_folder
        print("load data from folder", cloud_storage_folder)
        blob_context_files = get_all_documents_from_folder(BUCKET_NAME, cloud_storage_folder)
        for filename in blob_context_files.keys():
            f = open(filename, 'wb')
            f.write(blob_context_files[filename])
            f.close()


def generate_folder_name(uuid: str):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = today_str+"-"+uuid
    return cloud_storage_folder


def load_context_files(dbt_command: dbt_command, folder: str):
    write_to_bucket(BUCKET_NAME, folder+"/manifest.json", dbt_command.manifest)
    write_to_bucket(BUCKET_NAME, folder+"/dbt_project.yml", dbt_command.dbt_project)
    write_to_bucket(BUCKET_NAME, folder+"/profiles.yml", dbt_command.profiles)
