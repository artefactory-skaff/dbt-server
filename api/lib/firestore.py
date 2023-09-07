import os
from google.cloud import firestore
from google.oauth2.credentials import Credentials


def connect_firestore_collection():
    SERVICE_ACCOUNT_TOKEN = os.getenv('SERVICE_ACCOUNT_KEY', default='')
    if SERVICE_ACCOUNT_TOKEN != '':
        cred = Credentials(token=SERVICE_ACCOUNT_TOKEN)
        client = firestore.Client(credentials=cred)
    else:
        client = firestore.Client()
    dbt_collection = client.collection("dbt-status")
    return dbt_collection
