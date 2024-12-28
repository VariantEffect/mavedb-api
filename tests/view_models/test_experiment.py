import pytest
from fastapi.encoders import jsonable_encoder

from mavedb.view_models.experiment import ExperimentCreate, SavedExperiment
from mavedb.view_models.publication_identifier import PublicationIdentifier

from tests.helpers.constants import (
    VALID_EXPERIMENT_URN,
    VALID_SCORE_SET_URN,
    VALID_EXPERIMENT_SET_URN,
    TEST_MINIMAL_EXPERIMENT,
    TEST_MINIMAL_EXPERIMENT_RESPONSE,
    SAVED_PUBMED_PUBLICATION,
    TEST_PUBMED_IDENTIFIER,
    TEST_BIORXIV_IDENTIFIER,
)
from tests.helpers.util import dummy_attributed_object_from_dict


# Test valid experiment
def test_create_experiment():
    experiment = ExperimentCreate(**jsonable_encoder(TEST_MINIMAL_EXPERIMENT))
    assert experiment.title == "Test Experiment Title"
    assert experiment.short_description == "Test experiment"
    assert experiment.abstract_text == "Abstract"
    assert experiment.method_text == "Methods"


def test_cannot_create_experiment_without_a_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"title"})
    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"title"})
    invalid_experiment["title"] = " "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_title():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"title"})
    invalid_experiment["title"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_without_a_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"shortDescription"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"shortDescription"})
    invalid_experiment["shortDescription"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"shortDescription"})
    invalid_experiment["shortDescription"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_without_an_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"abstractText"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"abstractText"})
    invalid_experiment["abstractText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"abstractText"})
    invalid_experiment["abstractText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_without_a_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"methodText"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"methodText"})
    invalid_experiment["methodText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"methodText"})
    invalid_experiment["methodText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "Input should be a valid string" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_multiple_primary_publications():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    experiment["primaryPublicationIdentifiers"] = [
        {"identifier": TEST_PUBMED_IDENTIFIER},
        {"identifier": TEST_BIORXIV_IDENTIFIER},
    ]
    invalid_experiment = jsonable_encoder(experiment)

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


@pytest.mark.parametrize("exclude", ["publication_identifier_associations", "score_sets", "experiment_set"])
def test_cannot_create_saved_experiment_without_all_attributed_properties(exclude):
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
    with pytest.raises(ValueError) as exc_info:
        SavedExperiment.model_validate(experiment_attributed_object)

    assert "Unable to create SavedExperiment without attribute" in str(exc_info.value)
    assert exclude in str(exc_info.value)
