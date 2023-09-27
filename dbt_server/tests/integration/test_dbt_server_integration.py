import time
from datetime import datetime, timedelta

from google.cloud import run_v2
from fastapi.testclient import TestClient

from dbt_server.dbt_server import app, create_job, launch_job
from dbt_server.lib.state import State, current_date_time
from dbt_server.lib.dbt_classes import DbtCommand
from dbt_server.lib.cloud_storage import CloudStorage, connect_client
from dbt_server.lib.firestore import connect_firestore_collection


client = TestClient(app)


def test_create_job():
    state = State("0000", CloudStorage(connect_client()), connect_firestore_collection())
    dbt_command = DbtCommand(
        server_url="https://dbt-server.com",
        user_command="test",
        processed_command="test",
        manifest="manifest",
        dbt_project="dbt_project"
    )

    job = create_job(state, dbt_command)
    assert job.name == "projects/stc-dbt-test-9e19/locations/europe-west9/jobs/u0000"
    assert check_job_creation(job)  # return True if client.get_job does not fail

    delete_job(job.name)


def test_launch_job():
    state = State("0000", CloudStorage(connect_client()), connect_firestore_collection())
    dbt_command = DbtCommand(
        server_url="https://dbt-server.com",
        user_command="test",
        processed_command="test",
        manifest="manifest",
        dbt_project="dbt_project"
    )

    job = create_job(state, dbt_command)
    launch_job(state, job)
    actual_job = get_job(job)
    print(actual_job)
    assert actual_job.execution_count == 1
    execution = actual_job.latest_created_execution
    print(execution.completion_time)

    while str(execution.completion_time) == "1970-01-01 00:00:00+00:00":
        time.sleep(1)
        actual_job = get_job(job)
        execution = actual_job.latest_created_execution

    delete_job(job.name)


def check_job_creation(response_job: run_v2.types.Job):
    get_job(response_job)
    return True


def get_job(response_job: run_v2.types.Job):
    request = run_v2.GetJobRequest(
        name=response_job.name,
    )

    client = run_v2.JobsClient()
    job = client.get_job(request=request)
    return job


def delete_job(job_name: str):
    client = run_v2.JobsClient()
    request = run_v2.DeleteJobRequest(
        name=job_name,
    )
    operation = client.delete_job(request=request)

    print("Waiting for deletion to complete...")
    operation.result()


def test_get_job_status():
    uuid = "test"
    state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())

    state.init_state()
    response = client.get(f"/job/{uuid}")
    assert response.status_code == 200
    assert response.json() == {"run_status": "created"}

    state.run_status = "pending"

    response = client.get(f"/job/{uuid}")
    assert response.status_code == 200
    assert response.json() == {"run_status": "pending"}


def test_get_last_logs():
    uuid = "test"
    state = State(uuid, CloudStorage(connect_client()), connect_firestore_collection())
    state.init_state()

    response = client.get(f"/job/{uuid}/last_logs")
    assert response.status_code == 200

    dt_time = current_date_time()
    logs = [dt_time+'\tINFO\tInit']
    logs_server = response.json()["run_logs"]
    assert are_logs_equal(logs, logs_server, timedelta(seconds=10))


def are_logs_equal(logs_1: list[str], logs_2: list[str], timedelta: timedelta) -> bool:
    if len(logs_1) != len(logs_2):
        return False
    for i in range(len(logs_1)):
        date_1, date_2 = logs_1[i].split('\t')[0], logs_2[i].split('\t')[0]
        if not are_dates_in_timedelta(date_1, date_2, timedelta):
            return False
        if logs_1[i].split('\t')[1:] != logs_2[i].split('\t')[1:]:
            return False
    return True


def are_dates_in_timedelta(date_str_1: str, date_str_2: str, timedelta: timedelta) -> bool:
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    date_obj_1 = datetime.strptime(date_str_1, date_format)
    date_obj_2 = datetime.strptime(date_str_2, date_format)

    if date_obj_1 > date_obj_2 and (date_obj_1 - date_obj_2) <= timedelta:
        return True
    elif date_obj_1 < date_obj_2 and (date_obj_2 - date_obj_1) <= timedelta:
        return True
    else:
        return False
