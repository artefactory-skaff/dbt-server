from config import Settings

from lib.logger import DbtLogger
from lib.cloud_storage import CloudStorageFactory, CloudStorage
from lib.metadata_document import MetadataDocumentFactory, MetadataDocument
from lib.state import State


settings = Settings()


METADATA_DOCUMENT = MetadataDocument(
    MetadataDocumentFactory().create(settings.metadata_document_service)
)
CLOUD_STORAGE_INSTANCE = CloudStorage(
    CloudStorageFactory().create(settings.cloud_storage_service)
)
LOGGER = DbtLogger(settings.logging_service, settings.uuid)
STATE = State(settings.uuid, CLOUD_STORAGE_INSTANCE, METADATA_DOCUMENT)
