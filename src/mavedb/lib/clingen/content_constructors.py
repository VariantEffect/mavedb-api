from datetime import datetime
from uuid import uuid4
from urllib.parse import quote_plus

from mavedb import __version__
from mavedb.constants import MAVEDB_BASE_GIT, MAVEDB_FRONTEND_URL
from mavedb.lib.types.clingen import LdhContentLinkedData, LdhContentSubject, LdhEvent, LdhSubmission
from mavedb.lib.clingen.constants import LDH_ENTITY_NAME, LDH_SUBMISSION_TYPE
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant


def construct_ldh_submission_event(sbj: LdhContentSubject) -> LdhEvent:
    return {
        "type": LDH_SUBMISSION_TYPE,
        "name": LDH_ENTITY_NAME,
        "uuid": str(uuid4()),
        "sbj": {"id": sbj["Variant"]["hgvs"], "type": "Variant", "format": "hgvs", "add": True},
        "triggered": {
            "by": {
                "host": MAVEDB_BASE_GIT,
                "id": "resource_published",
                "iri": f"{MAVEDB_BASE_GIT}/releases/tag/v{__version__}",
            },
            "at": datetime.now().isoformat(),
        },
    }


def construct_ldh_submission_subject(hgvs: str) -> LdhContentSubject:
    return {"Variant": {"hgvs": hgvs}}


def construct_ldh_submission_entity(variant: Variant, mapped_variant: MappedVariant) -> LdhContentLinkedData:
    return {
        # TODO#372: We try to make all possible fields that are non-nullable represented that way.
        "MaveDBMapping": [
            {
                "entContent": {
                    "mavedb_id": variant.urn,  # type: ignore
                    "pre_mapped": mapped_variant.pre_mapped,  # type: ignore
                    "post_mapped": mapped_variant.post_mapped,  # type: ignore
                    "mapping_api_version": mapped_variant.mapping_api_version,  # type: ignore
                    "score": variant.data["score_data"]["score"],  # type: ignore
                },
                "entId": variant.urn,  # type: ignore
                "entIri": f"{MAVEDB_FRONTEND_URL}/score-sets/{quote_plus(variant.score_set.urn)}?variant={quote_plus(variant.urn)}",  # type: ignore
            }
        ]
    }


def construct_ldh_submission(variant_content: list[tuple[str, Variant, MappedVariant]]) -> list[LdhSubmission]:
    content_submission: list[LdhSubmission] = []
    for hgvs, variant, mapped_variant in variant_content:
        subject = construct_ldh_submission_subject(hgvs)
        event = construct_ldh_submission_event(subject)
        entity = construct_ldh_submission_entity(variant, mapped_variant)

        content_submission.append(
            {
                "event": event,
                "content": {"sbj": subject, "ld": entity},
            }
        )

    return content_submission
