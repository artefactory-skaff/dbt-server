import argparse
import os

from server.lib.dbt_server import DBTServer
from server.lib.logger import get_logger


def main(args: argparse.Namespace):
    logger = get_logger(args.log_level)
    # storage backend and schedule backend should inherit a common class and we should pick which subclass based on
    # user input (provider)
    dbt_server = DBTServer(logger, args.port, storage_backend=None, schedule_backend=None)
    dbt_server.start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DBT server remote")
    parser.add_argument("--provider", "-p", help="Cloud provider the DBT server will be deployed to", default="GCP",
                        type=str)
    parser.add_argument("--log-level", help="Log level", default="INFO", type=str)
    parser.add_argument("--port", help="Port the server listens to", type=int, default=os.getenv("PORT", 8000))
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args)
