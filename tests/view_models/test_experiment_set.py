from mavedb.view_models.experiment_set import ExperimentSetCreate


# Test valid experiment controlled keyword
def test_create_experiment_set():
    urn = "urn:xxx-xxx-xxx"
    experiment_set = ExperimentSetCreate(
        urn=urn,
    )
    assert experiment_set.urn == urn
