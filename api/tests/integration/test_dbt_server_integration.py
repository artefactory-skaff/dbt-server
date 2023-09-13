import sys
from google.cloud import run_v2
import time

sys.path.insert(1, './api')
sys.path.insert(2, './api/lib')
from dbt_server import create_job, launch_job
from state import State
from dbt_classes import DbtCommand
from cloud_storage import CloudStorage, connect_client
from firestore import connect_firestore_collection


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
    assert job.name == "projects/stc-dbt-test-9e19/locations/us-central1/jobs/u0000"
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
