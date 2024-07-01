import logging


def get_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("dbt_server")
    logger_handler = logging.StreamHandler()
    logger_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")  # TODO: add structure
    logger_handler.setFormatter(logger_formatter)
    if not logger.hasHandlers():
        logger.addHandler(logger_handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger
