from typing import Union

from mavedb.models.collection import Collection
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User

# Define the supported entity types
EntityType = Union[
    Collection,
    Experiment,
    ExperimentSet,
    ScoreCalibration,
    ScoreSet,
    User,
]
