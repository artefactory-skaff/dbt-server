import firebase_admin
from firebase_admin import firestore


app = firebase_admin.initialize_app()
db = firestore.client()
dbt_collection = db.collection("dbt-status")


def set_status(id: str,status: str):
    doc_ref = dbt_collection.document(id)
    doc_ref.set({"uuid": id, "status": status})

def get_status(id: str):
    doc_ref = dbt_collection.document(id)
    return doc_ref.get().to_dict()

def get_all_status():
    docs = dbt_collection.stream()
    for doc in docs:
        print(f"{doc.id} => {doc.to_dict()}")
