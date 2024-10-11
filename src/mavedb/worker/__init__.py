from sqlalchemy.orm import configure_mappers

from mavedb.models import *  # noqa: F403
from mavedb.worker.settings import ArqWorkerSettings

# Scan all our model classes and create backref attributes. Otherwise, these attributes only get added to classes once
# an instance of the related class has been created. Since the worker is a distinct service, we should make sure this
# happens whenever a worker is spawned.
configure_mappers()

# Expose the worker settings to the arq CLI Worker initializer
global WorkerSettings
WorkerSettings = ArqWorkerSettings
