from typing import List, Dict, Tuple
from datetime import date, datetime, timezone
import logging
import traceback

from api.config import Settings
from api.lib.metadata_document import MetadataDocument
from api.lib.dbt_classes import DbtCommand
from api.lib.cloud_storage import CloudStorageFactory


settings = Settings()
CLOUD_STORAGE_INSTANCE = CloudStorageFactory().create(settings.cloud_storage_service)


class State:
    def __init__(self, uuid: str, metadata_document: MetadataDocument):
        self._uuid = uuid
        self.run_logs = DbtRunLogs(uuid)
        self.run_logs_buffer = []
        self.metadata_document = metadata_document
        initial_state = {
            "uuid": self._uuid,
            "run_status": "created",
            "user_command": "",
            "cloud_storage_folder": "",
            "log_starting_byte": 0,
        }
        self.metadata_document.create(initial_state)
        self.run_logs.init_log_file()

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def run_status(self) -> str:
        run_status = self.metadata_document.get().to_dict()["run_status"]
        return run_status

    @run_status.setter
    def run_status(self, new_status: str):
        self.metadata_document.update({"run_status": new_status})

    @property
    def user_command(self) -> str:
        run_status = self.metadata_document.get().to_dict()["user_command"]
        return run_status

    @user_command.setter
    def user_command(self, user_command: str):
        self.metadata_document.update({"user_command": user_command})

    @property
    def log_starting_byte(self) -> int:
        log_starting_byte = self.metadata_document.get().to_dict()["log_starting_byte"]
        return log_starting_byte

    @log_starting_byte.setter
    def log_starting_byte(self, new_log_starting_byte: int):
        self.metadata_document.update({"log_starting_byte": new_log_starting_byte})

    @property
    def cloud_storage_folder(self) -> str:
        cloud_storage_folder = self.metadata_document.get().to_dict()["cloud_storage_folder"]
        return cloud_storage_folder

    @cloud_storage_folder.setter
    def cloud_storage_folder(self, cloud_storage_folder: str):
        self.metadata_document.update({"cloud_storage_folder": cloud_storage_folder})

    def load_context(self, dbt_command: DbtCommand) -> None:
        cloud_storage_folder = generate_folder_name(self._uuid)
        logging.info("cloud_storage_folder " + cloud_storage_folder)
        self.cloud_storage_folder = cloud_storage_folder
        CLOUD_STORAGE_INSTANCE.write_file(
            settings.bucket_name, cloud_storage_folder + "/manifest.json", dbt_command.manifest
        )
        CLOUD_STORAGE_INSTANCE.write_file(
            settings.bucket_name,
            cloud_storage_folder + "/dbt_project.yml",
            dbt_command.dbt_project,
        )
        if dbt_command.packages is not None:
            CLOUD_STORAGE_INSTANCE.write_file(
                settings.bucket_name,
                cloud_storage_folder + "/packages.yml",
                dbt_command.packages,
            )
        if dbt_command.seeds is not None:
            for seed_name in dbt_command.seeds.keys():
                seed_str = dbt_command.seeds[seed_name]
                CLOUD_STORAGE_INSTANCE.write_file(
                    settings.bucket_name, cloud_storage_folder + "/" + seed_name, seed_str
                )

    def get_context_to_local(self) -> None:
        cloud_storage_folder = self.cloud_storage_folder
        logging.info("load data from folder " + cloud_storage_folder)
        blob_context_files = CLOUD_STORAGE_INSTANCE.get_files_in_folder(
            settings.bucket_name, cloud_storage_folder
        )
        write_files(blob_context_files)
        blob_seed_files = CLOUD_STORAGE_INSTANCE.get_files_in_folder(
            settings.bucket_name, cloud_storage_folder + "/seeds"
        )
        write_files(blob_seed_files, "seeds/")

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
        new_log = dt_time + "\t" + severity + "\t" + new_log

        self.run_logs_buffer.append(new_log)
        self.run_logs.log(self.run_logs_buffer)


class DbtRunLogs:
    def __init__(self, uuid: str):
        self._uuid = uuid
        self.log_file = "logs/" + uuid + ".txt"

    def init_log_file(self) -> None:
        dt_time = current_date_time()
        CLOUD_STORAGE_INSTANCE.write_file(
            settings.bucket_name, self.log_file, dt_time + "\t" + "INFO" + "\t" + "Init"
        )

    def get(self, starting_byte: int = 0) -> Tuple[List[str], int]:
        current_log_file = CLOUD_STORAGE_INSTANCE.get_file(
            settings.bucket_name, self.log_file, starting_byte
        )
        byte_length = len(current_log_file)
        if byte_length == 0:
            return [], byte_length
        run_logs = current_log_file.decode("utf-8").rstrip("\n").split("\n")
        return run_logs, byte_length

    def log(self, logs: List[str]) -> None:
        new_log_file = "\n".join(logs)
        try:
            CLOUD_STORAGE_INSTANCE.write_file(settings.bucket_name, self.log_file, new_log_file)
        except Exception:
            traceback_str = traceback.format_exc()
            print("Error", "Error uploading log to bucket")
            print(traceback_str)


def write_files(files: Dict[str, bytes], prefix: str = ""):
    for filename in files.keys():
        try:
            with open(prefix + filename, "wb") as f:
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
    cloud_storage_folder = today_str + "-" + uuid
    return cloud_storage_folder
