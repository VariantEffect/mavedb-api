import logging
from typing import Optional
import urllib.parse

from ga4gh.core.models import iriReference as IRI
from ga4gh.va_spec.base.core import Document

from mavedb.constants import MAVEDB_FRONTEND_URL
from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.mapped_variant import MappedVariant

logger = logging.getLogger(__name__)


def experiment_as_iri(experiment: Experiment) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB experiment. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{MAVEDB_FRONTEND_URL}/experiments/{experiment.urn}")


def experiment_to_document(experiment: Experiment) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB experiment.
    """
    return Document(
        id=experiment.urn,
        name="MaveDB Experiment",
        title=experiment.title,
        description=experiment.short_description,
        documentType="experiment",
        urls=[experiment_as_iri(experiment).root],
    )


def score_set_as_iri(score_set: ScoreSet) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB score set. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{MAVEDB_FRONTEND_URL}/score-sets/{score_set.urn}")


def score_set_to_document(score_set: ScoreSet) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB score set.
    """
    return Document(
        id=score_set.urn,
        name="MaveDB Score Set",
        title=score_set.title,
        description=score_set.short_description,
        documentType="score set",
        urls=[score_set_as_iri(score_set).root],
    )


def mapped_variant_as_iri(mapped_variant: MappedVariant) -> Optional[IRI]:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB mapped variant. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    if not mapped_variant.clingen_allele_id:
        return None

    return IRI(f"https://mavedb.org/variant/{urllib.parse.quote_plus(mapped_variant.clingen_allele_id)}")


def mapped_variant_to_document(mapped_variant: MappedVariant) -> Optional[Document]:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB mapped variant.
    """
    if not mapped_variant.clingen_allele_id:
        return None

    return Document(
        id=mapped_variant.variant.urn,
        name="MaveDB Mapped Variant",
        documentType="mapped genomic variant description",
        # We only reach this point if a IRI is guaranteed to exist
        urls=[mapped_variant_as_iri(mapped_variant).root],  # type: ignore
    )


def variant_as_iri(variant: Variant) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB variant. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    # TODO(#372): nullable URN.
    return IRI(f"https://mavedb.org/score-sets/{variant.score_set.urn}?variant={urllib.parse.quote_plus(variant.urn)}")  # type: ignore


def variant_to_document(variant: Variant) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB variant.
    """
    return Document(
        id=variant.urn,
        name="MaveDB Variant",
        documentType="genomic variant description",
        urls=[variant_as_iri(variant).root],
    )
