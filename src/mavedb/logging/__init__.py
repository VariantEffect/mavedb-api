import logging
import logging.config
import os
import sys

from .config import load_stock_config
from .filters import canonical_only
from .formatters import MavedbJsonFormatter

WATCHTOWER_IMPORTED = False
try:
    from watchtower import CloudWatchLogHandler

    WATCHTOWER_IMPORTED = True
except ModuleNotFoundError:
    pass

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
    cw_is_enabled = False
    root_logger = logging.getLogger("root")

    if WATCHTOWER_IMPORTED:
        for handler in root_logger.handlers:
            if isinstance(handler, CloudWatchLogHandler):
                handler.addFilter(canonical_only)
                handler.formatter = MavedbJsonFormatter()

                cw_is_enabled = True

    if not cw_is_enabled:
        root_logger.info("CloudWatch log handler is not enabled. Canonical logs will only be emitted to stdout.")
