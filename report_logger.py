import logging
from logging.handlers import TimedRotatingFileHandler
import configparser as cp


def get_logger(name):
    config = cp.ConfigParser()
    config.read("report.properties")
    logger_path = config['config']['logger_path']
    log = logging.getLogger("MDC_report." + name)
    handler = TimedRotatingFileHandler(logger_path, when="midnight", interval=1)
    handler.suffix = "%Y%m%d"
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log
