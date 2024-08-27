import logging
from typing import Any

from pythonjsonlogger.jsonlogger import JsonFormatter


class MavedbJsonFormatter(JsonFormatter):
    def add_fields(self, log_record: dict[str, Any], record: logging.LogRecord, message_dict: dict[str, Any]) -> None:
        """
        Override JsonFormatter to add level and filepath to emitted messages.
        """
        message_dict["level"] = record.levelname
        message_dict["filepath"] = record.pathname

        super(MavedbJsonFormatter, self).add_fields(log_record, record, message_dict)
