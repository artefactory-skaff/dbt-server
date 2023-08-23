import os
from google.cloud import firestore
from dbt_types import dbt_command
from datetime import date
from cloud_storage import write_to_bucket, get_document_from_bucket, get_all_documents_from_folder
import logging
from datetime import datetime, timezone

BUCKET_NAME = os.getenv('BUCKET_NAME')
MAX_LOGS = 200  # max number of logs to keep in Firestore

client = firestore.Client()
dbt_collection = client.collection("dbt-status")


class State:

    def __init__(self, uuid: str):
        self._uuid = uuid
        self.run_logs = Run_logs(uuid)

    def init_state(self):
        status_ref = dbt_collection.document(self._uuid)
        initial_state = {
            "uuid": self._uuid,
            "run_status": "created",
            "user_command": "",
            "log_level": "info",
            "cloud_storage_folder": "",
            "log_starting_byte": 0
        }
        status_ref.set(initial_state)
        self.run_logs.init_log_file()

    @property
    def uuid(self):
        return self._uuid

    @property
    def run_status(self):
        status_ref = dbt_collection.document(self._uuid)
        run_status = status_ref.get().to_dict()["run_status"]
        return run_status

    @run_status.setter
    def run_status(self, new_status: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"run_status": new_status})

    @property
    def user_command(self):
        status_ref = dbt_collection.document(self._uuid)
        run_status = status_ref.get().to_dict()["user_command"]
        return run_status

    @user_command.setter
    def user_command(self, user_command: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"user_command": user_command})

    @property
    def log_level(self):
        status_ref = dbt_collection.document(self._uuid)
        log_level = status_ref.get().to_dict()["log_level"]
        return log_level

    @log_level.setter
    def log_level(self, new_log_level: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"log_level": new_log_level})

    @property
    def log_starting_byte(self):
        status_ref = dbt_collection.document(self._uuid)
        log_starting_byte = status_ref.get().to_dict()["log_starting_byte"]
        return log_starting_byte

    @log_starting_byte.setter
    def log_starting_byte(self, new_log_starting_byte: int):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"log_starting_byte": new_log_starting_byte})

    @property
    def storage_folder(self):
        status_ref = dbt_collection.document(self._uuid)
        cloud_storage_folder = status_ref.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @storage_folder.setter
    def storage_folder(self, cloud_storage_folder: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"cloud_storage_folder": cloud_storage_folder})

    def load_context(self, dbt_command: dbt_command):
        cloud_storage_folder = generate_folder_name(self._uuid)
        logging.info('cloud_storage_folder ' + cloud_storage_folder)
        self.storage_folder = cloud_storage_folder
        write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/manifest.json", dbt_command.manifest)
        write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/dbt_project.yml", dbt_command.dbt_project)
        if dbt_command.packages is not None:
            write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/packages.yml", dbt_command.packages)

    def get_context_to_local(self):
        cloud_storage_folder = self.storage_folder
        logging.info("load data from folder " + cloud_storage_folder)
        blob_context_files = get_all_documents_from_folder(BUCKET_NAME, cloud_storage_folder)
        for filename in blob_context_files.keys():
            with open(filename, 'wb') as f:
                f.write(blob_context_files[filename])

    def get_last_logs(self):
        logs, byte_length = self.run_logs.get(self.log_starting_byte)
        self.log_starting_byte += byte_length
        return logs


class Run_logs:

    def __init__(self, uuid: str):
        self._uuid = uuid
        self.log_file = 'logs/' + uuid + '.txt'

    def init_log_file(self):
        dt_time = current_date_time()
        write_to_bucket(BUCKET_NAME, self.log_file, dt_time + "\t" + "INFO" + "\t" + "Init")

    def get(self, starting_byte: int = 0):
        current_log_file = get_document_from_bucket(BUCKET_NAME, self.log_file, starting_byte)
        byte_length = len(current_log_file)
        if byte_length == 0:
            return [], byte_length
        run_logs = current_log_file.decode('utf-8').split('\n')
        return run_logs, byte_length

    def debug(self, new_log: str):
        self.__add(severity="DEBUG", new_log=new_log)

    def info(self, new_log: str):
        self.__add(severity="INFO", new_log=new_log)

    def warn(self, new_log: str):
        self.__add(severity="WARN", new_log=new_log)

    def error(self, new_log: str):
        self.__add(severity="ERROR", new_log=new_log)

    def __add(self, severity: str, new_log: str):
        current_log_file = get_document_from_bucket(BUCKET_NAME, self.log_file).decode('utf-8')

        dt_time = current_date_time()
        new_log = (dt_time + "\t" + severity + "\t" + new_log)

        new_log_file = current_log_file + '\n' + new_log
        write_to_bucket(BUCKET_NAME, self.log_file, new_log_file)


def current_date_time():
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string


def generate_folder_name(uuid: str):
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = today_str+"-"+uuid
    return cloud_storage_folder
