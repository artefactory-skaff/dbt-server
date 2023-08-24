import os


def set_env_vars():
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    DOCKER_IMAGE = os.getenv('DOCKER_IMAGE')
    SERVICE_ACCOUNT = os.getenv('SERVICE_ACCOUNT')
    PROJECT_ID = os.getenv('PROJECT_ID')
    LOCATION = os.getenv('LOCATION')
    return BUCKET_NAME, DOCKER_IMAGE, SERVICE_ACCOUNT, PROJECT_ID, LOCATION
