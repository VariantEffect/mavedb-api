from typing import NamedTuple, Sequence, Union

from mavedb.models.experiment import Experiment
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.variant import Variant

# Non-exhaustive
ResourceWithCreationModificationDates = Union[ExperimentSet, Experiment, ScoreSet, MappedVariant, Variant]

PublicationIdentifierAssociations = Union[
    Sequence[ScoreSetPublicationIdentifierAssociation],
    Sequence[ExperimentPublicationIdentifierAssociation],
]


class SequenceFeature(NamedTuple):
    """Identifier and coding system for a mapped sequence feature."""

    identifier: str
    system: str
