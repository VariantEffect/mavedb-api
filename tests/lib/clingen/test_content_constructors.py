from unittest.mock import patch
from uuid import UUID
from urllib.parse import quote_plus

from mavedb.constants import MAVEDB_BASE_GIT, MAVEDB_FRONTEND_URL
from mavedb.lib.clingen.content_constructors import (
    construct_ldh_submission_event,
    construct_ldh_submission_subject,
    construct_ldh_submission,
    construct_ldh_submission_entity,
)
from mavedb.lib.clingen.constants import LDH_ENTITY_NAME, LDH_SUBMISSION_TYPE
from mavedb import __version__
import pytest

from tests.helpers.constants import (
    TEST_HGVS_IDENTIFIER,
    VALID_VARIANT_URN,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    VALID_SCORE_SET_URN,
)


def test_construct_ldh_submission_subject():
    result = construct_ldh_submission_subject(TEST_HGVS_IDENTIFIER)
    assert result == {"Variant": {"hgvs": TEST_HGVS_IDENTIFIER}}


def test_construct_ldh_submission_event():
    sbj = construct_ldh_submission_subject(TEST_HGVS_IDENTIFIER)

    with (
        patch("mavedb.lib.clingen.content_constructors.uuid4") as mock_uuid4,
    ):
        mock_uuid4.return_value = UUID("12345678-1234-5678-1234-567812345678")

        result = construct_ldh_submission_event(sbj)

        assert result["type"] == LDH_SUBMISSION_TYPE
        assert result["name"] == LDH_ENTITY_NAME
        assert result["uuid"] == "12345678-1234-5678-1234-567812345678"
        assert result["sbj"] == {
            "id": TEST_HGVS_IDENTIFIER,
            "type": "Variant",
            "format": "hgvs",
            "add": True,
            "iri": None
        }
        assert result["triggered"]["by"] == {
            "host": MAVEDB_BASE_GIT,
            "id": "resource_published",
            "iri": f"{MAVEDB_BASE_GIT}/releases/tag/v{__version__}",
        }


@pytest.mark.parametrize("has_mapped_variant", [(True), (False)])
def test_construct_ldh_submission_entity(mock_variant, mock_mapped_variant, has_mapped_variant: bool):
    mapped_variant = mock_mapped_variant if has_mapped_variant else None
    result = construct_ldh_submission_entity(mock_variant, mapped_variant)

    assert "MaveDBMapping" in result
    assert len(result["MaveDBMapping"]) == 1
    mapping = result["MaveDBMapping"][0]

    assert mapping["entContent"]["mavedb_id"] == VALID_VARIANT_URN
    assert mapping["entContent"]["score"] == 1.0

    if has_mapped_variant:
        assert mapping["entContent"]["pre_mapped"] == TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X
        assert mapping["entContent"]["post_mapped"] == TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
        assert mapping["entContent"]["mapping_api_version"] == "pytest.mapping.1.0"
    else:
        assert "pre_mapped" not in mapping["entContent"]
        assert "post_mapped" not in mapping["entContent"]
        assert "mapping_api_version" not in mapping["entContent"]

    assert mapping["entId"] == VALID_VARIANT_URN
    assert (
        mapping["entIri"]
        == f"{MAVEDB_FRONTEND_URL}/score-sets/{quote_plus(VALID_SCORE_SET_URN)}?variant={quote_plus(VALID_VARIANT_URN)}"
    )


@pytest.mark.parametrize("has_mapped_variant", [(True), (False)])
def test_construct_ldh_submission(mock_variant, mock_mapped_variant, has_mapped_variant: bool):
    mapped_variant = mock_mapped_variant if has_mapped_variant else None
    variant_content = [
        (TEST_HGVS_IDENTIFIER, mock_variant, mapped_variant),
        (TEST_HGVS_IDENTIFIER, mock_variant, mapped_variant),
    ]

    uuid_1 = UUID("12345678-1234-5678-1234-567812345678")
    uuid_2 = UUID("87654321-4321-8765-4321-876543218765")

    with (
        patch("mavedb.lib.clingen.content_constructors.uuid4") as mock_uuid4,
    ):
        mock_uuid4.side_effect = [
            uuid_1,
            uuid_2,
        ]

        result = construct_ldh_submission(variant_content)

        assert len(result) == 2

        # Validate the first submission
        submission1 = result[0]
        assert submission1["event"]["uuid"] == str(uuid_1)
        assert submission1["event"]["sbj"]["id"] == TEST_HGVS_IDENTIFIER
        assert submission1["content"]["sbj"] == {"Variant": {"hgvs": TEST_HGVS_IDENTIFIER}}
        assert submission1["content"]["ld"]["MaveDBMapping"][0]["entContent"]["mavedb_id"] == VALID_VARIANT_URN
        assert submission1["content"]["ld"]["MaveDBMapping"][0]["entContent"]["score"] == 1.0

        # Validate the second submission
        submission2 = result[1]
        assert submission2["event"]["uuid"] == str(uuid_2)
        assert submission2["event"]["sbj"]["id"] == TEST_HGVS_IDENTIFIER
        assert submission2["content"]["sbj"] == {"Variant": {"hgvs": TEST_HGVS_IDENTIFIER}}
        assert submission2["content"]["ld"]["MaveDBMapping"][0]["entContent"]["mavedb_id"] == VALID_VARIANT_URN
        assert submission2["content"]["ld"]["MaveDBMapping"][0]["entContent"]["score"] == 1.0
