from functools import cache

from google.cloud import firestore

def get_collection(collection_name: str) -> firestore.CollectionReference:
    return get_client().collection(collection_name)

@cache
def get_client() -> firestore.Client:
    return firestore.Client()
