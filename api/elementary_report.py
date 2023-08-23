import os

from state import State
from new_logger import init_logger
from cloud_storage import write_to_bucket

logger = init_logger()


def main():

    request_uuid = os.environ.get("UUID")
    state = State(request_uuid)

    log = "elementary_report started"
    Log_info(state, log)

    elementary = os.environ.get("ELEMENTARY")
    if elementary == "True":
        log = "Uploading report..."
        Log_info(state, log)
        upload_elementary_report(state)
    else:
        log = "Elementary not requested"
        Log_info(state, log)

    log = "END REPORT"
    Log_info(state, log)


def upload_elementary_report(state: State):
    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')

    cloud_storage_folder = state.storage_folder

    with open('edr_target/elementary_output.json', 'r') as f:
        elementary_output = f.read()
    with open('edr_target/elementary_report.html', 'r') as f:
        elementary_report = f.read()

    write_to_bucket(bucket_name, cloud_storage_folder+"/elementary_output.json", elementary_output)
    write_to_bucket(bucket_name, cloud_storage_folder+"/elementary_report.html", elementary_report)


def Log_info(state: State, log: str):
    logger.info(log)
    state.run_logs.info(log)


if __name__ == '__main__':
    main()
