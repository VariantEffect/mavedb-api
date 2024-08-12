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
    invalid_experiment = {
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_title():
    invalid_experiment = {
        "title": " ",
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_title():
    invalid_experiment = {
        "title": "",
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "none is not an allowed value" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_experiment_without_a_short_description():
    invalid_experiment = {
        "title": "title",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_short_description():
    invalid_experiment = {
        "title": "title",
        "shortDescription": " ",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_short_description():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "",
        "abstractText": "Abstract",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "none is not an allowed value" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_experiment_without_an_abstract():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_abstract():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "abstractText": " ",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_abstract():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "abstractText": "",
        "methodText": "Methods",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "none is not an allowed value" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_experiment_without_a_method():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_a_space_method():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
        "methodText": " ",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_experiment_with_an_empty_method():
    invalid_experiment = {
        "title": "title",
        "shortDescription": "Test experiment",
        "abstractText": "Abstract",
        "methodText": "",
    }

    with pytest.raises(ValueError) as exc_info:
        ExperimentCreate(**jsonable_encoder(invalid_experiment))

    assert "none is not an allowed value" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)
