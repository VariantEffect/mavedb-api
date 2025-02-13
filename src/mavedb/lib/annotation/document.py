import logging
import urllib.parse

from ga4gh.core.entity_models import Document, IRI

from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)

# TODO: move centrally
BASE_URL = "https://mavedb.org"


def experiment_as_iri(experiment: Experiment) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB experiment. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{BASE_URL}/experiments/{experiment.urn}")


def experiment_to_document(experiment: Experiment) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB experiment.
    """
    return Document(
        id=experiment.urn,
        label="MaveDB experiment",
        title=experiment.title,
        urls=[experiment_as_iri(experiment)],
    )


def score_set_as_iri(score_set: ScoreSet) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB score set. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{BASE_URL}/score-sets/{score_set.urn}")


def score_set_to_document(score_set: ScoreSet) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB score set.
    """
    return Document(
        id=score_set.urn,
        label="MaveDB score set",
        title=score_set.title,
        urls=[score_set_as_iri(score_set)],
    )


def variant_as_iri(variant: Variant) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB variant. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    # TODO: We should decide if this should instead link to the variant measurement page.
    # TODO(#372): nullable URN.
    return f"https://mavedb.org/score-sets/{variant.score_set.urn}?variant={urllib.parse.quote_plus(variant.urn)}"  # type: ignore


def variant_to_document(variant: Variant) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB variant.
    """
    return Document(
        id=variant.urn,
        label="MaveDB variant",
        urls=[variant_as_iri(variant)],
    )
