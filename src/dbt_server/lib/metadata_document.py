from typing import Any, Dict

import json
import os
from abc import ABC, abstractmethod

from dbt_server.config import Settings

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore
try:
    from azure.cosmos import CosmosClient, exceptions
except ImportError:
    CosmosClient = None  # type: ignore
    exceptions = None  # type: ignore


settings = Settings()


class MetadataDocument(ABC):
    @abstractmethod
    def get(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create(self, data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def update(self, data: Dict[str, Any]) -> None:
        pass


class LocalDocument(MetadataDocument):
    def __init__(self, collection_name, document_id):
        self.path = f"{collection_name}/{document_id}"

    def get(self) -> Dict[str, Any]:
        with open(self.path) as file:
            return json.load(file)

    def create(self, data: Dict[str, Any]) -> None:
        with open(self.path, "w") as file:
            json.dump(data, file)

    def update(self, data: Dict[str, Any]) -> None:
        old_data = {}
        with open(self.path) as file:
            old_data = json.load(file)
        new_data = old_data | data
        with open(self.path, "w") as file:
            json.dump(new_data, file)


class FirestoreDocument(MetadataDocument):
    def __init__(self, collection_name, document_id):
        self.document = firestore.Client().client.collection(collection_name).document(document_id)

    def get(self) -> Dict[str, Any]:
        return self.document.get().to_dict()

    def create(self, data: Dict[str, Any]) -> None:
        self.document.set(data)

    def update(self, data: Dict[str, Any]) -> None:
        self.document.update(data)


class CosmosDBDocument(MetadataDocument):
    def __init__(self, collection_name, document_id):
        self.client = CosmosClient(
            settings.azure.cosmos_db_url, credential=settings.azure.cosmos_db_key
        )
        self.database = self.client.get_database_client(settings.azure.cosmos_db_database)
        self.container = self.database.get_container_client(collection_name)
        self.document_id = document_id

    def get(self):
        try:
            return self.container.read_item(item=self.document_id, partition_key=self.document_id)
        except exceptions.CosmosHttpResponseError as e:
            print(e.message)

    def create(self, data: Dict[str, Any]) -> None:
        try:
            data["id"] = data["uuid"]
            self.container.upsert_item(body=data)
        except exceptions.CosmosHttpResponseError as e:
            print(e.message)
            print(data)

    def update(self, data: Dict[str, Any]) -> None:
        try:
            previous_data = self.get()
            new_data = previous_data | data
            new_data["id"] = new_data["uuid"]
            self.container.replace_item(item=self.document_id, body=new_data)
        except exceptions.CosmosHttpResponseError as e:
            print(e.message)


class MetadataDocumentFactory:
    @staticmethod
    def create(service_type: str, collection_name: str, document_id: str) -> MetadataDocument:
        if service_type == "Firestore":
            return FirestoreDocument(collection_name, document_id)
        elif service_type == "CosmosDB":
            return CosmosDBDocument(collection_name, document_id)
        elif service_type == "Local":
            return LocalDocument(collection_name, document_id)
        else:
            raise ValueError("Invalid service type")
