import logging
import sys


class LoggerSetup:
    def __init__(
        self,
        logger_name: str,
        logging_level: str = "info",
        logging_format: dict = None,
    ) -> None:
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level=getattr(logging, logging_level.upper()))
        handler = logging.StreamHandler(sys.stdout)

        log_format = logging_format or {
            "log_format": (
                "%(asctime)s | %(levelname)s | %(filename)s "
                "| %(funcName)s | %(lineno)s : %(message)s"
            ),
            "date_format": "%Y-%m-%d %H:%M:%S",
        }
        # https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles
        formatter = logging.Formatter(
            log_format["log_format"], datefmt=log_format["date_format"]
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def get_logger(self) -> logging.Logger:
        '''
        This function will just initialize LoggerSetup Class object while calling
        constructor which will return logger object with a specific format, if 
        not provided.
        :return: logging.logger object with a specific logging level and a format 
        '''
        return self.logger
