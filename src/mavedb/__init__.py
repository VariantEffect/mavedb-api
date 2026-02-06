import logging as module_logging

import mavedb.logging as application_logging

application_logging.configure()
logger = module_logging.getLogger(__name__)

__project__ = "mavedb-api"
__version__ = "2026.1.0"

logger.info(f"MaveDB {__version__}")

# Import the model rebuild module to ensure all view model forward references are resolved
from mavedb.view_models import model_rebuild  # noqa: F401, E402
