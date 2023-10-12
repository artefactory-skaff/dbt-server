import os
from typing import List, Dict, Tuple
from datetime import date, datetime, timezone
import logging
import traceback
from uuid import uuid4
import yaml

from lib.firestore import get_collection
from lib.dbt_classes import DbtCommand
from lib.cloud_storage import CloudStorage

with open("lib/server_default_config.yml", 'r') as f:
    SERVER_DEFAULT_CONFIG = yaml.safe_load(f)

BUCKET_NAME = os.getenv('BUCKET_NAME', default=SERVER_DEFAULT_CONFIG["bucket_name"])


class State:

    def __init__(self, uuid: str = None):
        self.uuid = str(uuid4()) if uuid is None else uuid

        self.run_logs = DbtRunLogs(self.uuid)
        self.cloud_storage_instance = CloudStorage()
        self.dbt_collection = get_collection("dbt-status")

        self.run_logs_buffer = []

        if uuid is None:
            self.init_state()

    @classmethod
    def from_uuid(cls, uuid: str):
        state = cls(uuid)
        return state

    def init_state(self):
        status_ref = self.dbt_collection.document(self.uuid)
        initial_state = {
            "uuid": self.uuid,
            "run_status": "created",
            "user_command": "",
            "cloud_storage_folder": "",
            "log_starting_byte": 0
        }
        status_ref.set(initial_state)
        self.run_logs.init_log_file()

    @property
    def run_status(self) -> str:
        status_ref = self.dbt_collection.document(self.uuid)
        run_status = status_ref.get().to_dict()["run_status"]
        return run_status

    @run_status.setter
    def run_status(self, new_status: str):
        status_ref = self.dbt_collection.document(self.uuid)
        status_ref.update({"run_status": new_status})

    @property
    def user_command(self) -> str:
        status_ref = self.dbt_collection.document(self.uuid)
        run_status = status_ref.get().to_dict()["user_command"]
        return run_status

    @user_command.setter
    def user_command(self, user_command: str):
        status_ref = self.dbt_collection.document(self.uuid)
        status_ref.update({"user_command": user_command})

    @property
    def log_starting_byte(self) -> int:
        status_ref = self.dbt_collection.document(self.uuid)
        log_starting_byte = status_ref.get().to_dict()["log_starting_byte"]
        return log_starting_byte

    @log_starting_byte.setter
    def log_starting_byte(self, new_log_starting_byte: int):
        status_ref = self.dbt_collection.document(self.uuid)
        status_ref.update({"log_starting_byte": new_log_starting_byte})

    @property
    def cloud_storage_folder(self) -> str:
        status_ref = self.dbt_collection.document(self.uuid)
        cloud_storage_folder = status_ref.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @cloud_storage_folder.setter
    def cloud_storage_folder(self, cloud_storage_folder: str):
        status_ref = self.dbt_collection.document(self.uuid)
        status_ref.update({"cloud_storage_folder": cloud_storage_folder})

    def load_context(self, dbt_command: DbtCommand) -> None:
        cloud_storage_folder = generate_folder_name(self.uuid)
        logging.info('cloud_storage_folder ' + cloud_storage_folder)
        self.cloud_storage_folder = cloud_storage_folder
        self.cloud_storage_instance.write_to_bucket(BUCKET_NAME,
                                                    cloud_storage_folder+"/manifest.json", dbt_command.manifest)
        self.cloud_storage_instance.write_to_bucket(BUCKET_NAME,
                                                    cloud_storage_folder+"/dbt_project.yml", dbt_command.dbt_project)
        self.cloud_storage_instance.write_to_bucket(BUCKET_NAME,
                                                    cloud_storage_folder+"/profiles.yml", dbt_command.profiles)
        if dbt_command.packages is not None:
            self.cloud_storage_instance.write_to_bucket(BUCKET_NAME,
                                                        cloud_storage_folder+"/packages.yml", dbt_command.packages)
        if dbt_command.seeds is not None:
            for seed_name in dbt_command.seeds.keys():
                seed_str = dbt_command.seeds[seed_name]
                self.cloud_storage_instance.write_to_bucket(BUCKET_NAME, cloud_storage_folder+"/"+seed_name, seed_str)

    def get_context_to_local(self) -> None:
        cloud_storage_folder = self.cloud_storage_folder
        logging.info(f"load data from folder {cloud_storage_folder}")
        blob_context_files = self.cloud_storage_instance.get_all_blobs_from_folder(BUCKET_NAME, cloud_storage_folder)
        write_files(blob_context_files)
        blob_seed_files = self.cloud_storage_instance.get_all_blobs_from_folder(BUCKET_NAME,
                                                                                cloud_storage_folder+'/seeds')
        write_files(blob_seed_files, 'seeds/')

    def get_last_logs(self) -> List[str]:
        logs, byte_length = self.run_logs.get(self.log_starting_byte)
        if byte_length != 0:
            self.log_starting_byte += byte_length + 1
        return logs

    def get_all_logs(self) -> List[str]:
        logs, _ = self.run_logs.get(0)
        return logs

    def log(self, severity: str, new_log: str) -> None:
        if self.run_logs_buffer == []:
            all_previous_logs = self.get_all_logs()
            self.run_logs_buffer = all_previous_logs

        dt_time = current_date_time()
        new_log = (f"{dt_time}\t{severity}\t{new_log}")

        self.run_logs_buffer.append(new_log)
        self.run_logs.log(self.run_logs_buffer)


class DbtRunLogs:

    def __init__(self, uuid: str):
        self.uuid = uuid

        self.log_file = f'logs/{uuid}.txt'
        self.cloud_storage_instance = CloudStorage()

    def init_log_file(self) -> None:
        dt_time = current_date_time()
        self.cloud_storage_instance.write_to_bucket(BUCKET_NAME, self.log_file, dt_time+"\tINFO\tInit")

    def get(self, starting_byte: int = 0) -> Tuple[List[str], int]:
        current_log_file = self.cloud_storage_instance.get_blob_from_bucket(BUCKET_NAME, self.log_file, starting_byte)
        byte_length = len(current_log_file)
        if byte_length == 0:
            return [], byte_length
        run_logs = current_log_file.decode('utf-8').split('\n')
        return run_logs, byte_length

    def log(self, logs: List[str]) -> None:
        new_log_file = '\n'.join(logs)
        try:
            self.cloud_storage_instance.write_to_bucket(BUCKET_NAME, self.log_file, new_log_file)
        except Exception:
            traceback_str = traceback.format_exc()
            print("Error", "Error uploading log to bucket")
            print(traceback_str)


def write_files(files: Dict[str, bytes], prefix: str = ""):
    for filename in files.keys():
        try:
            with open(prefix + filename, 'wb') as f:
                f.write(files[filename])
        except Exception:
            traceback_str = traceback.format_exc()
            print("ERROR", f"Couldn't write file {filename}")
            print(traceback_str)


def current_date_time() -> str:
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string


def generate_folder_name(uuid: str) -> str:
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = f"{today_str}-{uuid}"
    return cloud_storage_folder
