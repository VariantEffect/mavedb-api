from app.crud.base import CrudBase
from app.models.experiment import Experiment
from app.view_models.experiment import ExperimentCreate, ExperimentUpdate


class CrudExperiment(CrudBase[Experiment, ExperimentCreate, ExperimentUpdate]):
    ...


experiment = CrudExperiment(Experiment)
