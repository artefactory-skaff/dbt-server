from enum import Enum
import uuid
from typing import Optional
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingServiceEnum(str, Enum):
    azure_monitor = "AzureMonitor"
    google_cloud_logging = "GoogleCloudLogging"
    local = "Local"


class StorageServiceEnum(str, Enum):
    azure_blob_storage = "AzureBlobStorage"
    google_cloud_storage = "GoogleCloudStorage"
    local_storage = "LocalStorage"


class MetadataDocumentServiceEnum(str, Enum):
    cosmos_db = "CosmosDB"
    firestore = "Firestore"
    local = "Local"


class JobServiceEnum(str, Enum):
    container_apps_job = "ContainerAppsJob"
    cloud_run_job = "CloudRunJob"
    local_job = "LocalJob"


class GCPSettings(BaseModel):
    project_id: str
    service_account: str
    location: str


class AzureSettings(BaseModel):
    resource_group_name: str
    location: str
    blob_storage_connection_string: str
    cosmos_db_url: str
    cosmos_db_database: str
    cosmos_db_key: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")
    docker_image: str
    bucket_name: str
    collection_name: str
    logging_service: LoggingServiceEnum
    storage_service: StorageServiceEnum
    metadata_document_service: MetadataDocumentServiceEnum
    job_service: JobServiceEnum
    dbt_command: str = ""
    port: int = 8001
    uuid: str = str(uuid.UUID(int=0))
    elementary: bool = False
    gcp: Optional[GCPSettings] = None
    azure: Optional[AzureSettings] = None

    @model_validator(mode="after")
    def check_if_at_least_one_cloud_settings_is_provided(self):
        gcp_validity = True
        azure_validity = True
        errors = []
        if self.job_service == "CloudRunJob" and not self.gcp:
            errors.append(ValueError(f"GCP config is not valid : {self}"))
        if (
            self.job_service == "ContainerAppsJob"
            or self.storage_service == "AzureBlobStorage"
            or self.metadata_document_service == "CosmosDB"
        ) and not self.azure:
            errors.append(ValueError(f"Azure config is not valid : {self}"))
        if errors:
            raise ExceptionGroup("Check the following configuration issues :", errors)