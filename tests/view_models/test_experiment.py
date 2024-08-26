import pytest
from fastapi.encoders import jsonable_encoder

from mavedb.view_models.experiment import ExperimentCreate
from tests.helpers.constants import TEST_MINIMAL_EXPERIMENT


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

    assert "field required" in str(exc_info.value)
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

    assert "none is not an allowed value" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_without_a_short_description():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"shortDescription"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "field required" in str(exc_info.value)
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

    assert "none is not an allowed value" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_without_an_abstract():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"abstractText"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "field required" in str(exc_info.value)
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

    assert "none is not an allowed value" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_without_a_method():
    experiment = TEST_MINIMAL_EXPERIMENT.copy()
    invalid_experiment = jsonable_encoder(experiment, exclude={"methodText"})

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**invalid_experiment)

    assert "field required" in str(exc_info.value)
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

    assert "none is not an allowed value" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)
