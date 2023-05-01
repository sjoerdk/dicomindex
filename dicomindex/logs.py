import logging

ROOT_LOGGER_NAME = "dicomindex"


def get_module_logger(name):
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")
