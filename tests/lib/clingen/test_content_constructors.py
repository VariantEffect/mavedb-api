from unittest.mock import patch
from uuid import UUID

from mavedb.lib.clingen.content_constructors import (
    construct_ldh_submission_event,
    construct_ldh_submission_subject,
    construct_ldh_submission,
    construct_ldh_submission_entity,
)
from mavedb.lib.clingen.constants import LDH_ENTITY_NAME, LDH_SUBMISSION_TYPE, MAVEDB_BASE_GIT
from mavedb import __version__


def test_construct_ldh_submission_event():
    sbj = {"Variant": {"hgvs": "NM_000546.5:c.215C>G"}}

    with (
        patch("mavedb.lib.clingen.content_constructors.uuid4") as mock_uuid4,
        patch("mavedb.lib.clingen.content_constructors.datetime") as mock_datetime,
    ):
        mock_uuid4.return_value = UUID("12345678-1234-5678-1234-567812345678")
        mock_datetime.now.return_value = "2023-01-01T00:00:00"

        result = construct_ldh_submission_event(sbj)

        assert result["type"] == LDH_SUBMISSION_TYPE
        assert result["name"] == LDH_ENTITY_NAME
        assert result["uuid"] == "12345678-1234-5678-1234-567812345678"
        assert result["sbj"] == {
            "id": "NM_000546.5:c.215C>G",
            "type": "Variant",
            "format": "hgvs",
            "add": True,
        }
        assert result["triggered"]["by"] == {
            "host": MAVEDB_BASE_GIT,
            "id": "resource_published",
            "iri": f"{MAVEDB_BASE_GIT}/releases/tag/v{__version__}",
        }
        assert result["triggered"]["at"] == "2023-01-01T00:00:00"


def test_construct_ldh_submission_subject():
    hgvs = "NM_000546.5:c.215C>G"
    result = construct_ldh_submission_subject(hgvs)

    assert result == {"Variant": {"hgvs": hgvs}}


def test_construct_ldh_submission_entity():
    class MockVariant:
        def __init__(self, urn, data):
            self.urn = urn
            self.data = data

    class MockMappedVariant:
        def __init__(self, pre_mapped, post_mapped, mapping_api_version):
            self.pre_mapped = pre_mapped
            self.post_mapped = post_mapped
            self.mapping_api_version = mapping_api_version

    variant = MockVariant(
        urn="urn:example:variant123",
        data={"score_data": {"score": 0.95}},
    )
    mapped_variant = MockMappedVariant(
        pre_mapped="pre-mapped-value",
        post_mapped="post-mapped-value",
        mapping_api_version="v1.0",
    )

    result = construct_ldh_submission_entity(variant, mapped_variant)

    assert "MaveDBMapping" in result
    assert len(result["MaveDBMapping"]) == 1
    mapping = result["MaveDBMapping"][0]

    assert mapping["entContent"]["mavedb_id"] == "urn:example:variant123"
    assert mapping["entContent"]["pre_mapped"] == "pre-mapped-value"
    assert mapping["entContent"]["post_mapped"] == "post-mapped-value"
    assert mapping["entContent"]["mapping_api_version"] == "v1.0"
    assert mapping["entContent"]["score"] == 0.95

    assert mapping["entId"] == "urn:example:variant123"
    assert mapping["entIri"] == "https://staging.mavedb.org/score-sets/urn:example:variant123"


def test_construct_ldh_submission():
    class MockVariant:
        def __init__(self, urn, data):
            self.urn = urn
            self.data = data

    class MockMappedVariant:
        def __init__(self, pre_mapped, post_mapped, mapping_api_version):
            self.pre_mapped = pre_mapped
            self.post_mapped = post_mapped
            self.mapping_api_version = mapping_api_version

    variant1 = MockVariant(
        urn="urn:example:variant123",
        data={"score_data": {"score": 0.95}},
    )
    mapped_variant1 = MockMappedVariant(
        pre_mapped="pre-mapped-value1",
        post_mapped="post-mapped-value1",
        mapping_api_version="v1.0",
    )

    variant2 = MockVariant(
        urn="urn:example:variant456",
        data={"score_data": {"score": 0.85}},
    )
    mapped_variant2 = MockMappedVariant(
        pre_mapped="pre-mapped-value2",
        post_mapped="post-mapped-value2",
        mapping_api_version="v2.0",
    )

    variant_content = [
        ("NM_000546.5:c.215C>G", variant1, mapped_variant1),
        ("NM_000546.5:c.216C>T", variant2, mapped_variant2),
    ]

    with (
        patch("mavedb.lib.clingen.content_constructors.uuid4") as mock_uuid4,
        patch("mavedb.lib.clingen.content_constructors.datetime") as mock_datetime,
    ):
        mock_uuid4.side_effect = [
            UUID("12345678-1234-5678-1234-567812345678"),
            UUID("87654321-4321-8765-4321-876543218765"),
        ]
        mock_datetime.now.return_value = "2023-01-01T00:00:00"

        result = construct_ldh_submission(variant_content)

        assert len(result) == 2

        # Validate the first submission
        submission1 = result[0]
        assert submission1["event"]["uuid"] == "12345678-1234-5678-1234-567812345678"
        assert submission1["event"]["sbj"]["id"] == "NM_000546.5:c.215C>G"
        assert submission1["content"]["sbj"] == {"Variant": {"hgvs": "NM_000546.5:c.215C>G"}}
        assert submission1["content"]["ld"]["MaveDBMapping"][0]["entContent"]["mavedb_id"] == "urn:example:variant123"
        assert submission1["content"]["ld"]["MaveDBMapping"][0]["entContent"]["score"] == 0.95

        # Validate the second submission
        submission2 = result[1]
        assert submission2["event"]["uuid"] == "87654321-4321-8765-4321-876543218765"
        assert submission2["event"]["sbj"]["id"] == "NM_000546.5:c.216C>T"
        assert submission2["content"]["sbj"] == {"Variant": {"hgvs": "NM_000546.5:c.216C>T"}}
        assert submission2["content"]["ld"]["MaveDBMapping"][0]["entContent"]["mavedb_id"] == "urn:example:variant456"
        assert submission2["content"]["ld"]["MaveDBMapping"][0]["entContent"]["score"] == 0.85
