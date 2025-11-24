import logging
import json
import datetime


class JsonFormatter(logging.Formatter):
    """
    Custom logging formatter to output logs in JSON format.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record as a JSON string.
        """
        log_entry = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def init_logging():
    """
    Initialize logging with JSON formatting.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
