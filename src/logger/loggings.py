import os
import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

class LoggerFactory:
    
    _LOG = None
    
    @staticmethod
    def __create_logger(log_file, log_level):
        LoggerFactory._LOG = logging.getLogger(log_file)
        
        LOG_PATH = "/home/data/news_crawling/logs"
        formatter = "%(asctime)s.%(msecs)04d - %(levelname)s - [%(filename)s:%(lineno)d] %(message)s"
        
        LOG_FILENAME = f"{LOG_PATH}/{log_file}_{datetime.now().strftime('%Y%m%d')}.log"
        rotating_file_handler = RotatingFileHandler(
            filename=LOG_FILENAME,
            maxBytes=2**20 # 1MB,
            backupCount=5
        )
        logging.basicConfig(
            level=log_level,
            format=formatter,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        
        if not os.path.exists(LOG_PATH):
            os.makedirs(LOG_PATH)
            
        if log_level == "DEBUG":
            LoggerFactory._LOG.setLevel(logging.DEBUG)
        elif log_level == "INFO":
            LoggerFactory._LOG.setLevel(logging.INFO)
        elif log_level == "WARNING":
            LoggerFactory._LOG.setLevel(logging.WARNING)
        elif log_level == "ERROR":
            LoggerFactory._LOG.setLevel(logging.ERROR)
        elif log_level == "CRITICAL":
            LoggerFactory._LOG.setLevel(logging.CRITICAL)
        
        LoggerFactory._LOG.addHandler(rotating_file_handler)
        
        return LoggerFactory._LOG
        
    
    @staticmethod
    def get_logger(log_file, log_level):
        logger = LoggerFactory.__create_logger(log_file, log_level)
        return logger