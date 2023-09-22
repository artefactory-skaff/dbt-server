import os


def pytest_configure(config):
    os.system("mkdir ./data")
    os.system("mkdir ./data/document")
    os.system("mkdir ./data/storage")
    os.system("mkdir ./data/storage/logs")


def pytest_unconfigure(config):
    os.system("rm -rf ./data")
