import pytest
from unittest.mock import Mock, patch
from dbt_server.lib.metadata_document import (
    MetadataDocument,
    FirestoreDocument,
    CosmosDBDocument,
    MetadataDocumentFactory,
)


@patch("dbt_server.lib.metadata_document.firestore")
def test_firestore_document_methods(mock_firestore):
    doc = FirestoreDocument("collection", "doc_id")
    doc.get()
    mock_firestore.Client().client.collection().document().get.assert_called_once()
    doc.create({"key": "value"})
    mock_firestore.Client().client.collection().document().set.assert_called_once_with(
        {"key": "value"}
    )
    doc.update({"key": "value"})
    mock_firestore.Client().client.collection().document().update.assert_called_once_with(
        {"key": "value"}
    )


@patch("dbt_server.lib.metadata_document.CosmosClient")
@patch("dbt_server.lib.metadata_document.settings")
def test_cosmos_db_document_methods(settings, mock_client):
    settings.azure.cosmos_db_url.return_value = None
    settings.azure.cosmos_db_key.return_value = None
    settings.azure.cosmos_db_database.return_value = None
    doc = CosmosDBDocument("collection", "doc_id")
    doc.get()
    mock_client().get_database_client().get_container_client().read_item.assert_called_once()
    doc.create({"key": "value"})
    mock_client().get_database_client().get_container_client().upsert_item.assert_called_once_with(
        body={"key": "value"}
    )
    doc.update({"key": "value"})
    mock_client().get_database_client().get_container_client().replace_item.assert_called_once()


@patch("dbt_server.lib.metadata_document.firestore")
@patch("dbt_server.lib.metadata_document.CosmosClient")
@patch("dbt_server.lib.metadata_document.settings")
def test_metadata_document_factory_create(settings, _, mock_firestore):
    mock_firestore.Client().client.collection().document().return_value = None
    doc = MetadataDocumentFactory.create("Firestore", "collection", "doc_id")
    assert isinstance(doc, FirestoreDocument)
    settings.azure.cosmos_db_url.return_value = None
    settings.azure.cosmos_db_key.return_value = None
    settings.azure.cosmos_db_database.return_value = None
    doc = MetadataDocumentFactory.create("CosmosDB", "collection", "doc_id")
    assert isinstance(doc, CosmosDBDocument)
    with pytest.raises(ValueError):
        MetadataDocumentFactory.create("InvalidService", "collection", "doc_id")
