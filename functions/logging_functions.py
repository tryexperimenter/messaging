import sys
import logging
from logging import FileHandler

# Log handler for writing to the console
def get_console_handler(log_format):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    return console_handler

# Log handler for writing to a file
def get_file_handler(log_file_path, log_format):
    file_handler = FileHandler(filename=log_file_path)
    file_handler.setFormatter(log_format)
    return file_handler

# Return a logger
def get_logger(log_file_path, logger_name='Default Logger'):

    log_format = logging.Formatter(
        "%(asctime)s — %(levelname)s: %(message)s")
    logger = logging.getLogger(logger_name)
    # better to have too much log than not enough
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler(log_format=log_format))
    logger.addHandler(get_file_handler(
        log_file_path=log_file_path, log_format=log_format))
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger

# Close a logger (including releasing any file handlers so you can delete the files as necessary)
def close_logger(logger):

    if logger is None:
        return

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
