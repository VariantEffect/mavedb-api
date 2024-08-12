import logging
import logging.config
import os
import sys

from watchtower import CloudWatchLogHandler

from .config import load_stock_config
from .filters import canonical_only
from .formatters import MavedbJsonFormatter

LOG_CONFIG = os.environ.get("LOG_CONFIG")


def configure():
    """
    Configures logging for MaveDB.

    A stock configuration is loaded from the ``mavedb/logging/configurations/*.yaml``
    files, chosen based on the value of ``LOG_CONFIG``.

    Python library warnings are captured and logged at the ``WARNING`` level.
    Uncaught exceptions are logged at the ``CRITICAL`` level before they cause
    the process to exit.
    """
    stock_config = load_stock_config(LOG_CONFIG if LOG_CONFIG else "default")
    logging.config.dictConfig(stock_config)

    # Log library API warnings.
    logging.captureWarnings(True)

    # Log any uncaught exceptions which are about to cause process exit.
    sys.excepthook = lambda *args: logging.getLogger().critical("Uncaught exception:", exc_info=args)  # type: ignore

    # Formatter and handler are un-configurable via file config.
    for handler in logging.getLogger("root").handlers:
        if isinstance(handler, CloudWatchLogHandler):
            handler.addFilter(canonical_only)
            handler.formatter = MavedbJsonFormatter()
