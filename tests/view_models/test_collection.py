import pytest
from pydantic import ValidationError

from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.view_models.collection import Collection, SavedCollection
from tests.helpers.constants import TEST_COLLECTION_RESPONSE
from tests.helpers.util.common import dummy_attributed_object_from_dict


@pytest.mark.parametrize(
    "exclude,expected_missing_fields",
    [
        ("user_associations", ["admins", "editors", "viewers"]),
        ("score_sets", ["scoreSetUrns"]),
        ("experiments", ["experimentUrns"]),
    ],
)
def test_cannot_create_saved_experiment_without_all_attributed_properties(exclude, expected_missing_fields):
    collection = TEST_COLLECTION_RESPONSE.copy()
    collection["urn"] = "urn:mavedb:collection-xxx"

    # Remove pre-existing synthetic properties
    collection.pop("experimentUrns", None)
    collection.pop("scoreSetUrns", None)
    collection.pop("admins", None)
    collection.pop("editors", None)
    collection.pop("viewers", None)

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    collection["experiments"] = [dummy_attributed_object_from_dict({"urn": "urn:mavedb:experiment-xxx"})]
    collection["score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:mavedb:score_set-xxx", "superseding_score_set": None})
    ]
    collection["user_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.admin,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.editor,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.viewer,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
    ]

    collection.pop(exclude)
    collection_attributed_object = dummy_attributed_object_from_dict(collection)
    with pytest.raises(ValidationError) as exc_info:
        SavedCollection.model_validate(collection_attributed_object)

    # Should fail with missing fields coerced from missing attributed properties
    msg = str(exc_info.value)
    assert "Field required" in msg
    for field in expected_missing_fields:
        assert field in msg


def test_saved_collection_can_be_created_with_all_attributed_properties():
    collection = TEST_COLLECTION_RESPONSE.copy()
    urn = "urn:mavedb:collection-xxx"
    collection["urn"] = urn

    # Remove pre-existing synthetic properties
    collection.pop("experimentUrns", None)
    collection.pop("scoreSetUrns", None)
    collection.pop("admins", None)
    collection.pop("editors", None)
    collection.pop("viewers", None)

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    collection["experiments"] = [dummy_attributed_object_from_dict({"urn": "urn:mavedb:experiment-xxx"})]
    collection["score_sets"] = [
        dummy_attributed_object_from_dict({"urn": "urn:mavedb:score_set-xxx", "superseding_score_set": None})
    ]
    collection["user_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.admin,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.editor,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "contribution_role": ContributionRole.viewer,
                "user": {"id": 1, "username": "test_user", "email": "test_user@example.com"},
            }
        ),
    ]

    collection_attributed_object = dummy_attributed_object_from_dict(collection)
    model = SavedCollection.model_validate(collection_attributed_object)
    assert model.name == TEST_COLLECTION_RESPONSE["name"]
    assert model.urn == urn
    assert len(model.admins) == 1
    assert len(model.editors) == 1
    assert len(model.viewers) == 1
    assert len(model.experiment_urns) == 1
    assert len(model.score_set_urns) == 1


def test_collection_can_be_created_from_non_orm_context():
    data = dict(TEST_COLLECTION_RESPONSE)
    data["urn"] = "urn:mavedb:collection-xxx"
    model = Collection.model_validate(data)
    assert model.urn == data["urn"]
