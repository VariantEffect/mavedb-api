"""
Environment setup for scripts.
"""

import logging

from sqlalchemy.orm import configure_mappers, Session

from mavedb import deps
from mavedb.models import *

def init_script_environment() -> Session:
    """
    Set up the environment for a script that may be run from the command line and does not necessarily depend on the
    FastAPI framework.

    Features:
    - Configures logging for the script.
    - Loads the SQLAlchemy data model.
    - Returns an SQLAlchemy database session.
    """

    logging.basicConfig()

    # Un-comment this line to log all database queries:
    logging.getLogger("__main__").setLevel(logging.INFO)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    # Scan all our model classes and create backref attributes. Otherwise, these attributes only get added to classes once
    # an instance of the related class has been created.
    configure_mappers()

    return next(deps.get_db())
