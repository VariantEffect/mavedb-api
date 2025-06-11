import logging as module_logging

import mavedb.logging as application_logging

application_logging.configure()
logger = module_logging.getLogger(__name__)

__project__ = "mavedb-api"
__version__ = "2025.2.0"

logger.info(f"MaveDB {__version__}")
