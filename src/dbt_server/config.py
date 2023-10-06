from typing import Optional

import uuid
from enum import Enum

from pydantic import BaseModel, BaseSettings, root_validator


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
    job_cpu: int = 1
    job_memory_in_gb: float = 1.5


class Settings(BaseSettings):
    class Config:
        env_nested_delimiter = "__"

    docker_image: str
    bucket_name: str
    collection_name: str
    logging_service: LoggingServiceEnum
    storage_service: StorageServiceEnum
    metadata_document_service: MetadataDocumentServiceEnum
    job_service: JobServiceEnum
    dbt_command: str = ""
    port: int = 8001
    uuid: str = str(uuid.uuid4())
    elementary: bool = False
    gcp: Optional[GCPSettings] = None
    azure: Optional[AzureSettings] = None

    @root_validator
    def check_cloud_specific_configurations(cls, values):
        gcp_validity = True
        azure_validity = True
        errors = []
        if values.get("job_service") == "CloudRunJob" and not values.get("gcp"):
            errors.append(ValueError(f"GCP config is not valid : {values}"))
        if (
            values.get("job_service") == "ContainerAppsJob"
            or values.get("storage_service") == "AzureBlobStorage"
            or values.get("metadata_document_service") == "CosmosDB"
        ) and not values.get("azure"):
            errors.append(ValueError(f"Azure config is not valid : {values}"))
        if errors:
            raise ExceptionGroup("Check the following configuration issues :", errors)
        return values
