import uuid
from typing import Optional
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingServiceEnum(str, Enum):
    azure_monitor = "AzureMonitor"
    google_cloud_logging = "GoogleCloudLogging"


class CloudStorageServiceEnum(str, Enum):
    azure_blob_storage = "AzureBlobStorage"
    google_cloud_storage = "GoogleCloudStorage"


class MetadataDocumentServiceEnum(str, Enum):
    cosmos_db = "CosmosDB"
    firestore = "Firestore"


class JobServiceEnum(str, Enum):
    container_apps_job = "ContainerAppsJob"
    cloud_run_job = "CloudRunJob"


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
    cloud_storage_service: CloudStorageServiceEnum
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
        if not self.gcp and not self.azure:
            raise ValueError("At least one of Azure or GCP settings must be provided !")
