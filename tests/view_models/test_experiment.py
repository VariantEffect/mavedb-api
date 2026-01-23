import pytest
from pydantic import ValidationError

from mavedb.view_models.experiment import Experiment, ExperimentCreate, SavedExperiment
from mavedb.view_models.publication_identifier import PublicationIdentifier
from tests.helpers.constants import (
    SAVED_BIORXIV_PUBLICATION,
    SAVED_PUBMED_PUBLICATION,
    TEST_BIORXIV_IDENTIFIER,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_EXPERIMENT_RESPONSE,
    TEST_PUBMED_IDENTIFIER,
    VALID_EXPERIMENT_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_SCORE_SET_URN,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict


# Test valid experiment
def test_create_experiment():
    experiment = ExperimentCreate(**TEST_MINIMAL_EXPERIMENT)
    assert experiment.title == "Test Experiment Title"
    assert experiment.short_description == "Test experiment"
    assert experiment.abstract_text == "Abstract"
    assert experiment.method_text == "Methods"


def test_cannot_create_experiment_without_a_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment.pop("title")
    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["title"] = " "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["title"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_without_a_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment.pop("shortDescription")

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["shortDescription"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["shortDescription"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_without_an_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment.pop("abstractText")

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["abstractText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["abstractText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_without_a_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment.pop("methodText")

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["methodText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["methodText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_multiple_primary_publications():
    invalid_experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment["primaryPublicationIdentifiers"] = [
        {"identifier": TEST_PUBMED_IDENTIFIER},
        {"identifier": TEST_BIORXIV_IDENTIFIER},
    ]

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Multiple primary publication identifiers are not allowed" in str(exc_info.value)
    assert "primaryPublicationIdentifiers" in str(exc_info.value)


def test_saved_experiment_synthetic_properties():
    experiment = TEST_MINIMAL_EXPERIMENT_RESPONSE.copy()
    experiment["urn"] = VALID_EXPERIMENT_URN

    # Remove pre-set synthetic properties
    experiment.pop("experimentSetUrn")
    experiment.pop("scoreSetUrns")
    experiment.pop("primaryPublicationIdentifiers")
    experiment.pop("secondaryPublicationIdentifiers")

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    experiment["experiment_set"] = dummy_attributed_object_from_dict({"urn": VALID_EXPERIMENT_SET_URN})
    experiment["score_sets"] = [
        dummy_attributed_object_from_dict({"urn": VALID_SCORE_SET_URN, "superseding_score_set": None})
    ]
    experiment["publication_identifier_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(**SAVED_PUBMED_PUBLICATION),
                "primary": True,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
    ]

    experiment_attributed_object = dummy_attributed_object_from_dict(experiment)
    saved_experiment = SavedExperiment.model_validate(experiment_attributed_object)

    # experiment_set_urn
    assert saved_experiment.experiment_set_urn == VALID_EXPERIMENT_SET_URN

    # score_set_urns
    assert len(saved_experiment.score_set_urns) == 1
    assert all([urn == VALID_SCORE_SET_URN for urn in saved_experiment.score_set_urns]) == 1

    # primary_publication_identifiers, secondary_publication_identifiers
    assert len(saved_experiment.primary_publication_identifiers) == 1
    assert len(saved_experiment.secondary_publication_identifiers) == 2
    assert all(
        [
            publication.identifier == TEST_PUBMED_IDENTIFIER
            for publication in saved_experiment.primary_publication_identifiers
        ]
    )
    assert all(
        [
            publication.identifier == TEST_BIORXIV_IDENTIFIER
            for publication in saved_experiment.secondary_publication_identifiers
        ]
    )


@pytest.mark.parametrize(
    "exclude,expected_missing_fields",
    [
        ("publication_identifier_associations", ["primaryPublicationIdentifiers", "secondaryPublicationIdentifiers"]),
        ("score_sets", ["scoreSetUrns"]),
        ("experiment_set", ["experimentSetUrn"]),
    ],
)
def test_cannot_create_saved_experiment_without_all_attributed_properties(exclude, expected_missing_fields):
    experiment = TEST_MINIMAL_EXPERIMENT_RESPONSE.copy()
    experiment["urn"] = VALID_EXPERIMENT_URN

    # Remove pre-set synthetic properties
    experiment.pop("experimentSetUrn")
    experiment.pop("scoreSetUrns")
    experiment.pop("primaryPublicationIdentifiers")
    experiment.pop("secondaryPublicationIdentifiers")

    # Set synthetic properties with dummy attributed objects to mock SQLAlchemy model objects.
    experiment["experiment_set"] = dummy_attributed_object_from_dict({"urn": VALID_EXPERIMENT_SET_URN})
    experiment["score_sets"] = [
        dummy_attributed_object_from_dict({"urn": VALID_SCORE_SET_URN, "superseding_score_set": None})
    ]
    experiment["publication_identifier_associations"] = [
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(**SAVED_PUBMED_PUBLICATION),
                "primary": True,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
        dummy_attributed_object_from_dict(
            {
                "publication": PublicationIdentifier(
                    **{**SAVED_PUBMED_PUBLICATION, **{"identifier": TEST_BIORXIV_IDENTIFIER}}
                ),
                "primary": False,
            }
        ),
    ]

    experiment.pop(exclude)
    experiment_attributed_object = dummy_attributed_object_from_dict(experiment)
    with pytest.raises(ValidationError) as exc_info:
        SavedExperiment.model_validate(experiment_attributed_object)

    # Should fail with missing fields coerced from missing attributed properties
    msg = str(exc_info.value)
    assert "Field required" in msg
    for field in expected_missing_fields:
        assert field in msg


def test_can_create_experiment_with_nonetype_experiment_set_urn():
    experiment_test = TEST_MINIMAL_EXPERIMENT.copy()
    experiment_test["experiment_set_urn"] = None
    experiment = ExperimentCreate(**experiment_test)

    assert experiment.experiment_set_urn is None


def test_cant_create_experiment_with_invalid_experiment_set_urn():
    experiment_test = TEST_MINIMAL_EXPERIMENT.copy()
    experiment_test["experiment_set_urn"] = "invalid_urn"

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**experiment_test)

    assert f"'{experiment_test['experiment_set_urn']}' is not a valid experiment set URN" in str(exc_info.value)


def test_can_create_experiment_from_non_orm_context():
    experiment = TEST_MINIMAL_EXPERIMENT_RESPONSE.copy()
    experiment["urn"] = VALID_EXPERIMENT_URN
    experiment["experimentSetUrn"] = VALID_EXPERIMENT_SET_URN
    experiment["scoreSetUrns"] = [VALID_SCORE_SET_URN]
    experiment["primaryPublicationIdentifiers"] = [SAVED_PUBMED_PUBLICATION]
    experiment["secondaryPublicationIdentifiers"] = [SAVED_PUBMED_PUBLICATION, SAVED_BIORXIV_PUBLICATION]

    # Should not require any ORM attributes
    saved_experiment = Experiment.model_validate(experiment)
    assert saved_experiment.urn == VALID_EXPERIMENT_URN
    assert saved_experiment.experiment_set_urn == VALID_EXPERIMENT_SET_URN
    assert saved_experiment.score_set_urns == [VALID_SCORE_SET_URN]
    assert len(saved_experiment.primary_publication_identifiers) == 1
    assert len(saved_experiment.secondary_publication_identifiers) == 2
