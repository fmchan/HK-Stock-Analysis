#-*- coding: utf-8 -*-
import logging
from datetime import datetime
from configs.settings import LOG_PATH
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

class Logger(object):
    def __init__(self, name):
        self.name = name
        self.log_formatter = logging.Formatter("%(levelname)-4s | %(asctime)s.%(msecs)03d | %(filename)s:%(lineno)03d | %(message)s", "%H:%M:%S")

    def setup_system_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(self.log_formatter)
        logger.addHandler(sh)

        Path(LOG_PATH).mkdir(parents=True, exist_ok=True)
        logname = LOG_PATH + "app.log"
        trfh = TimedRotatingFileHandler(logname, when="midnight", interval=1)
        trfh.setLevel(logging.INFO)
        trfh.setFormatter(self.log_formatter)
        trfh.suffix = "%Y%m%d"
        logger.addHandler(trfh)

        return logger

    def setup_simple_system_logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(self.log_formatter)
        logger.addHandler(sh)

        return logger