import os
from datetime import datetime
from logging.handlers import BaseRotatingHandler

class DailyCleanupHandler(BaseRotatingHandler):
    """A handler that creates a new log file daily and removes the previous one"""

    def __init__(self, filename, mode='a', encoding='utf-8'):
        super().__init__(filename, mode, encoding=encoding)
        self._last_day = self._get_current_day()

    def _get_current_day(self):
        """Returns the current date as a string in YYYY-MM-DD format"""
        return datetime.now().strftime("%Y-%m-%d")

    def shouldRollover(self, record):
        """Checks if the current date has changed"""
        current_day = self._get_current_day()
        if current_day != self._last_day:
            return True
        return False

    def doRollover(self):
        """
        Performs log file rollover:
        1. Closes the current file
        2. Deletes the previous log file
        3. Creates a new log file
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # Delete the previous log file
        if os.path.exists(self.baseFilename):
            os.remove(self.baseFilename)

        # Open new stream
        self.mode = 'a'
        self.stream = self._open()
        self._last_day = self._get_current_day()

def setup_logging(log_file, log_level):
    """Setup logging configuration with daily rotation"""
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "file": {
                "()": DailyCleanupHandler,
                "filename": log_file,
                "encoding": "utf-8",
                "formatter": "default"
            },
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            }
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "file"]
        }
    }
    return log_config