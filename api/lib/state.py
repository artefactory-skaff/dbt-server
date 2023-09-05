import os
from typing import List, Dict
from datetime import date, datetime, timezone
import logging
import traceback

from google.cloud import firestore

from lib.dbt_classes import DbtCommand
from lib.cloud_storage import write_to_bucket, get_blob_from_bucket, get_all_blobs_from_folder


BUCKET_NAME = os.getenv('BUCKET_NAME', default='dbt-stc-test')

client = firestore.Client()
dbt_collection = client.collection("dbt-status")


class State:

    def __init__(self, uuid: str):
        self._uuid = uuid
        self.run_logs = DbtRunLogs(uuid)
        self.run_logs_buffer = []

    def init_state(self):
        status_ref = dbt_collection.document(self._uuid)
        initial_state = {
            "uuid": self._uuid,
            "run_status": "created",
            "user_command": "",
            "cloud_storage_folder": "",
            "log_starting_byte": 0
        }
        status_ref.set(initial_state)
        self.run_logs.init_log_file()

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def run_status(self) -> str:
        status_ref = dbt_collection.document(self._uuid)
        run_status = status_ref.get().to_dict()["run_status"]
        return run_status

    @run_status.setter
    def run_status(self, new_status: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"run_status": new_status})

    @property
    def user_command(self) -> str:
        status_ref = dbt_collection.document(self._uuid)
        run_status = status_ref.get().to_dict()["user_command"]
        return run_status

    @user_command.setter
    def user_command(self, user_command: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"user_command": user_command})

    @property
    def log_starting_byte(self) -> int:
        status_ref = dbt_collection.document(self._uuid)
        log_starting_byte = status_ref.get().to_dict()["log_starting_byte"]
        return log_starting_byte

    @log_starting_byte.setter
    def log_starting_byte(self, new_log_starting_byte: int):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"log_starting_byte": new_log_starting_byte})

    @property
    def storage_folder(self) -> str:
        status_ref = dbt_collection.document(self._uuid)
        cloud_storage_folder = status_ref.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @storage_folder.setter
    def storage_folder(self, cloud_storage_folder: str):
        status_ref = dbt_collection.document(self._uuid)
        status_ref.update({"cloud_storage_folder": cloud_storage_folder})

    def load_context(self, dbt_command: DbtCommand) -> ():
        cloud_storage_folder = generate_folder_name(self._uuid)
        logging.info('cloud_storage_folder ' + cloud_storage_folder)
        self.storage_folder = cloud_storage_folder
        write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/manifest.json", dbt_command.manifest)
        write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/dbt_project.yml", dbt_command.dbt_project)
        if dbt_command.packages is not None:
            write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/packages.yml", dbt_command.packages)
        if dbt_command.seeds is not None:
            for seed_name in dbt_command.seeds.keys():
                seed_str = dbt_command.seeds[seed_name]
                write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/"+seed_name, seed_str)

    def get_context_to_local(self) -> ():
        cloud_storage_folder = self.storage_folder
        logging.info("load data from folder " + cloud_storage_folder)
        blob_context_files = get_all_blobs_from_folder(BUCKET_NAME, cloud_storage_folder)
        write_files(blob_context_files)
        blob_seed_files = get_all_blobs_from_folder(BUCKET_NAME, cloud_storage_folder+'/seeds')
        write_files(blob_seed_files, 'seeds/')

    def get_last_logs(self) -> List[str]:
        logs, byte_length = self.run_logs.get(self.log_starting_byte)
        if byte_length != 0:
            self.log_starting_byte += byte_length + 1
        return logs

    def get_all_logs(self) -> List[str]:
        logs, _ = self.run_logs.get(0)
        return logs

    def log(self, severity: str, new_log: str) -> ():
        if self.run_logs_buffer == []:
            all_previous_logs = self.get_all_logs()
            self.run_logs_buffer = all_previous_logs

        dt_time = current_date_time()
        new_log = (dt_time + "\t" + severity + "\t" + new_log)

        self.run_logs_buffer.append(new_log)
        self.run_logs.log(self.run_logs_buffer)


class DbtRunLogs:

    def __init__(self, uuid: str):
        self._uuid = uuid
        self.log_file = 'logs/' + uuid + '.txt'

    def init_log_file(self) -> ():
        dt_time = current_date_time()
        write_to_bucket(BUCKET_NAME, self.log_file, dt_time + "\t" + "INFO" + "\t" + "Init")

    def get(self, starting_byte: int = 0) -> (List[str], int):
        current_log_file = get_blob_from_bucket(BUCKET_NAME, self.log_file, starting_byte)
        byte_length = len(current_log_file)
        if byte_length == 0:
            return [], byte_length
        run_logs = current_log_file.decode('utf-8').split('\n')
        return run_logs, byte_length

    def log(self, logs: List[str]) -> ():
        new_log_file = '\n'.join(logs)
        write_to_bucket(BUCKET_NAME, self.log_file, new_log_file)


def write_files(files: Dict[str, bytes], prefix: str = ""):
    for filename in files.keys():
        try:
            with open(prefix + filename, 'wb') as f:
                f.write(files[filename])
        except Exception:
            traceback_str = traceback.format_exc()
            print("ERROR", "Couldn't write file" + filename)
            print(traceback_str)


def current_date_time() -> str:
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string


def generate_folder_name(uuid: str) -> str:
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = today_str+"-"+uuid
    return cloud_storage_folder
