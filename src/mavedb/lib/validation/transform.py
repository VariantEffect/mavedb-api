"""
Contains re-usable validators for fields which must be transformed prior to
being parsed into a Pydantic model. A replacement for the removed GetterDict
side-effect from v1.
"""

from typing import Optional, Sequence, Union
from typing_extensions import TypedDict

from pydantic import TypeAdapter

from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.collection_user_association import CollectionUserAssociation
from mavedb.models.experiment_publication_identifier import ExperimentPublicationIdentifierAssociation
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.models.score_calibration_publication_identifier import ScoreCalibrationPublicationIdentifierAssociation
from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.user import User
from mavedb.view_models.external_gene_identifier_offset import ExternalGeneIdentifierOffset
from mavedb.view_models.publication_identifier import PublicationIdentifier


# TODO(#372)
def transform_score_set_list_to_urn_list(score_sets: Optional[list[ScoreSet]]) -> list[Optional[str]]:
    if not score_sets:
        return []

    return [score_set.urn for score_set in score_sets if score_set.superseding_score_set is None]


def transform_experiment_list_to_urn_list(experiments: Optional[list[Experiment]]) -> list[Optional[str]]:
    if not experiments:
        return []

    return [experiment.urn for experiment in experiments]


def transform_experiment_set_to_urn(experiment_set: Optional[ExperimentSet]) -> Optional[str]:
    if not experiment_set:
        return None

    return experiment_set.urn


def transform_score_set_to_urn(score_set: Optional[ScoreSet]) -> Optional[str]:
    if not score_set:
        return None

    return score_set.urn


# TODO#523: Reduce code duplication during publication identifier transformation

RecordPublicationIdentifierAssociation = Union[
    ExperimentPublicationIdentifierAssociation, ScoreSetPublicationIdentifierAssociation
]


class TransformedScoreSetPublicationIdentifiers(TypedDict):
    primary_publication_identifiers: list[PublicationIdentifier]
    secondary_publication_identifiers: list[PublicationIdentifier]


class TransformedCalibrationPublicationIdentifiers(TypedDict):
    threshold_sources: list[PublicationIdentifier]
    classification_sources: list[PublicationIdentifier]
    method_sources: list[PublicationIdentifier]


def transform_record_publication_identifiers(
    publication_identifiers: Optional[Sequence[RecordPublicationIdentifierAssociation]],
) -> TransformedScoreSetPublicationIdentifiers:
    transformed_publication_identifiers = TransformedScoreSetPublicationIdentifiers(
        primary_publication_identifiers=[], secondary_publication_identifiers=[]
    )

    if not publication_identifiers:
        return transformed_publication_identifiers

    transformed_publication_identifiers["primary_publication_identifiers"] = [
        TypeAdapter(PublicationIdentifier).validate_python(assc.publication)
        for assc in publication_identifiers
        if assc.primary
    ]
    transformed_publication_identifiers["secondary_publication_identifiers"] = [
        TypeAdapter(PublicationIdentifier).validate_python(assc.publication)
        for assc in publication_identifiers
        if not assc.primary
    ]

    return transformed_publication_identifiers


def transform_score_calibration_publication_identifiers(
    publication_identifiers: Optional[Sequence[ScoreCalibrationPublicationIdentifierAssociation]],
) -> TransformedCalibrationPublicationIdentifiers:
    transformed_publication_identifiers = TransformedCalibrationPublicationIdentifiers(
        threshold_sources=[], classification_sources=[], method_sources=[]
    )

    if not publication_identifiers:
        return transformed_publication_identifiers

    transformed_publication_identifiers["threshold_sources"] = [
        TypeAdapter(PublicationIdentifier).validate_python(assc.publication)
        for assc in publication_identifiers
        if assc.relation is ScoreCalibrationRelation.threshold
    ]
    transformed_publication_identifiers["classification_sources"] = [
        TypeAdapter(PublicationIdentifier).validate_python(assc.publication)
        for assc in publication_identifiers
        if assc.relation is ScoreCalibrationRelation.classification
    ]
    transformed_publication_identifiers["method_sources"] = [
        TypeAdapter(PublicationIdentifier).validate_python(assc.publication)
        for assc in publication_identifiers
        if assc.relation is ScoreCalibrationRelation.method
    ]

    return transformed_publication_identifiers


def transform_external_identifier_offsets_to_list(data: TargetGene) -> list[ExternalGeneIdentifierOffset]:
    ensembl_offset = data.ensembl_offset
    refseq_offset = data.refseq_offset
    uniprot_offset = data.uniprot_offset
    return list(filter(lambda x: x is not None, [ensembl_offset, refseq_offset, uniprot_offset]))  # type:ignore


# We're unable to create a dynamically typed TypedDict which references enum keys and enforces totality of the enum, so this
# is simply typed as dict[str, User] to avoid coupling and the need to rewrite such a TypedDict any time the enum changes.
# See https://stackoverflow.com/a/72083153 for details about why this isn't possible.
def transform_contribution_role_associations_to_roles(
    user_associations: Sequence[CollectionUserAssociation],
) -> dict[str, list[User]]:
    contribution_roles: dict[str, list[User]] = {f"{k}s": [] for k in ContributionRole._member_names_}

    for assc in user_associations:
        contribution_roles[f"{assc.contribution_role.name}s"].append(assc.user)

    return contribution_roles
