import os
from tempfile import SpooledTemporaryFile
import tempfile
from typing import List, Dict, Tuple
from datetime import date, datetime, timezone
import logging
import traceback
from uuid import uuid4
import zipfile
from pathlib import Path

import yaml

from dbt_server.lib.firestore import get_collection
from dbt_server.lib.dbt_command import DbtCommand
from dbt_server.lib.gcs import CloudStorage

BUCKET_NAME = os.getenv('BUCKET_NAME')


class State:

    def __init__(self, dbt_command: DbtCommand = None, uuid: str = None):
        new_state = True if uuid is None else False
        if new_state and dbt_command is None:
            raise Exception("dbt_command must be provided when creating a new state")

        self.uuid = str(uuid4()) if uuid is None else uuid
        self.dbt_command = dbt_command

        self.run_logs = DbtRunLogs(self.uuid)
        self.gcs = CloudStorage(bucket_name=BUCKET_NAME)
        self.dbt_collection = get_collection("dbt-status")

        self.run_logs_buffer = []

        if new_state:
            self.init_state()

    @classmethod
    def from_uuid(cls, uuid: str):
        state = cls(uuid=uuid)
        return state

    @classmethod
    def from_schedule_uuid(cls, uuid: str):
        base_state = cls(uuid=uuid)
        original_state_document_contents = base_state.dbt_collection.document(uuid).get().to_dict()

        new_uuid = str(uuid4())
        new_state_document_contents = original_state_document_contents
        new_state_document_contents["uuid"] = new_uuid

        new_state_document = base_state.dbt_collection.document(new_uuid)
        new_state_document.set(new_state_document_contents)
        state = cls(uuid=new_uuid)
        return state

    def init_state(self):
        document = self.dbt_collection.document(self.uuid)
        initial_state = {
            "uuid": self.uuid,
            "run_status": "scheduled",
            "user_command": self.dbt_command.user_command,
            "dbt_native_params_overrides": self.dbt_command.dbt_native_params_overrides,
            "cloud_storage_folder": "",
            "log_starting_byte": 0
        }
        document.set(initial_state)
        self.cloud_storage_folder = generate_folder_name(self.uuid)
        self.run_logs.init_log_file()
        self.save_context_to_gcs()

    @property
    def run_status(self) -> str:
        document = self.dbt_collection.document(self.uuid)
        run_status = document.get().to_dict()["run_status"]
        return run_status

    @run_status.setter
    def run_status(self, new_status: str):
        status_ref = self.dbt_collection.document(self.uuid)
        status_ref.update({"run_status": new_status})

    @property
    def user_command(self) -> str:
        document = self.dbt_collection.document(self.uuid)
        user_command = document.get().to_dict()["user_command"]
        self._user_command = user_command
        return user_command

    @user_command.setter
    def user_command(self, user_command: str):
        document = self.dbt_collection.document(self.uuid)
        document.update({"user_command": user_command})

    @property
    def dbt_native_params_overrides(self) -> dict:
        document = self.dbt_collection.document(self.uuid)
        dbt_native_params_overrides = document.get().to_dict()["dbt_native_params_overrides"]
        return dbt_native_params_overrides

    @dbt_native_params_overrides.setter
    def dbt_native_params_overrides(self, dbt_native_params_overrides: dict):
        document = self.dbt_collection.document(self.uuid)
        document.update({"dbt_native_params_overrides": dbt_native_params_overrides})

    @property
    def log_starting_byte(self) -> int:
        document = self.dbt_collection.document(self.uuid)
        log_starting_byte = document.get().to_dict()["log_starting_byte"]
        return log_starting_byte

    @log_starting_byte.setter
    def log_starting_byte(self, new_log_starting_byte: int):
        document = self.dbt_collection.document(self.uuid)
        document.update({"log_starting_byte": new_log_starting_byte})

    @property
    def cloud_storage_folder(self) -> str:
        document = self.dbt_collection.document(self.uuid)
        cloud_storage_folder = document.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @cloud_storage_folder.setter
    def cloud_storage_folder(self, cloud_storage_folder: str):
        document = self.dbt_collection.document(self.uuid)
        document.update({"cloud_storage_folder": cloud_storage_folder})

    def extract_artifacts(self, zipped_artifacts: SpooledTemporaryFile) -> None:
        logging.info("cloud_storage_folder :" + self.cloud_storage_folder)
        with tempfile.TemporaryDirectory() as temp_dir:

            temp_dir_path = Path(temp_dir)
            artifacts_zip_path = temp_dir_path / 'artifacts.zip'
            with artifacts_zip_path.open('wb') as f:
                f.write(zipped_artifacts.file._file.getvalue())

            with zipfile.ZipFile(artifacts_zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            artifacts_zip_path.unlink()

            for file_path in temp_dir_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(temp_dir_path)
                    with open(file_path, 'r') as file:
                        data = file.read()
                    self.gcs.save(str(Path(self.cloud_storage_folder) / relative_path), data)

    def save_context_to_gcs(self) -> None:
        logging.info("cloud_storage_folder :" + self.cloud_storage_folder)
        self.gcs.save(self.cloud_storage_folder + "/dbt_project.yml", str(yaml.dump(self.dbt_command.dbt_project)))
        self.gcs.save(self.cloud_storage_folder + "/profiles.yml", str(yaml.dump(self.dbt_command.profiles)))
        self.gcs.save(self.cloud_storage_folder + "/packages.yml", str(yaml.dump(self.dbt_command.packages)))


    def save_context_to_local(self) -> None:
        logging.info(f"load data from folder {self.cloud_storage_folder}")

        blob_context_files = self.gcs.get_files_from_folder(self.cloud_storage_folder)
        write_files(blob_context_files)

        blob_seed_files = self.gcs.get_files_from_folder(self.cloud_storage_folder + '/seeds')
        write_files(blob_seed_files, 'seeds/')

    def get_last_logs(self) -> List[str]:
        logs, byte_length = self.run_logs.get(self.log_starting_byte)
        if byte_length != 0:
            self.log_starting_byte += byte_length + 1
        return logs

    def log(self, severity: str, new_log: str) -> None:
        if self.run_logs_buffer == []:
            all_previous_logs = self.get_all_logs()
            self.run_logs_buffer = all_previous_logs

        dt_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        new_log = (f"{dt_time}\t{severity}\t{new_log}")

        self.run_logs_buffer.append(new_log)
        self.run_logs.log(self.run_logs_buffer)

    def get_all_logs(self) -> List[str]:
        logs, _ = self.run_logs.get(0)
        return logs


class DbtRunLogs:

    def __init__(self, uuid: str):
        self.uuid = uuid

        self.log_file = f'logs/{uuid}.txt'
        self.gcs = CloudStorage(bucket_name=BUCKET_NAME)

    def init_log_file(self) -> None:
        dt_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.gcs.save(self.log_file, dt_time+"\tINFO\tInit")

    def get(self, starting_byte: int = 0) -> Tuple[List[str], int]:
        current_log_file = self.gcs.load(self.log_file, starting_byte)
        byte_length = len(current_log_file)
        if byte_length == 0:
            return [], byte_length
        run_logs = current_log_file.decode('utf-8').split('\n')
        return run_logs, byte_length

    def log(self, logs: List[str]) -> None:
        new_log_file = '\n'.join(logs)
        try:
            self.gcs.save(self.log_file, new_log_file)
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

def generate_folder_name(uuid: str) -> str:
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    cloud_storage_folder = f"{today_str}-{uuid}"
    return cloud_storage_folder
