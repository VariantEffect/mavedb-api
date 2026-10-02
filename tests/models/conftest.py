import pytest

from mavedb.models.enums import JobStatus
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.job_run import JobRun
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    TEST_EXPERIMENT,
    TEST_EXPERIMENT_SET,
    TEST_LICENSE,
    TEST_MINIMAL_VARIANT,
    TEST_SEQ_SCORESET,
    TEST_USER,
    VALID_EXPERIMENT_SET_URN,
    VALID_EXPERIMENT_URN,
    VALID_SCORE_SET_URN,
)


@pytest.fixture
def setup_lib_db_with_score_set(session, setup_lib_db):
    """Build an experiment set, experiment, and score set on top of the base lib db (users/licenses)."""
    user = session.query(User).filter(User.username == TEST_USER["username"]).first()

    experiment_set = ExperimentSet(**TEST_EXPERIMENT_SET, urn=VALID_EXPERIMENT_SET_URN)
    experiment_set.created_by = user
    experiment_set.modified_by = user
    session.add(experiment_set)
    session.commit()
    session.refresh(experiment_set)

    experiment = Experiment(**TEST_EXPERIMENT, urn=VALID_EXPERIMENT_URN, experiment_set_id=experiment_set.id)
    experiment.created_by = user
    experiment.modified_by = user
    session.add(experiment)
    session.commit()
    session.refresh(experiment)

    score_set_scaffold = TEST_SEQ_SCORESET.copy()
    score_set_scaffold.pop("target_genes")
    score_set = ScoreSet(
        **score_set_scaffold, urn=VALID_SCORE_SET_URN, experiment_id=experiment.id, licence_id=TEST_LICENSE["id"]
    )
    score_set.created_by = user
    score_set.modified_by = user
    session.add(score_set)
    session.commit()
    session.refresh(score_set)

    return score_set


@pytest.fixture
def setup_lib_db_with_variant(session, setup_lib_db_with_score_set):
    """Add a single variant to the score set, for variant-subject event tests."""
    variant = Variant(
        **TEST_MINIMAL_VARIANT, urn=f"{setup_lib_db_with_score_set.urn}#1", score_set_id=setup_lib_db_with_score_set.id
    )
    session.add(variant)
    session.commit()
    session.refresh(variant)

    return variant


@pytest.fixture
def job_run(session):
    """Create a persisted JobRun to anchor an event's provenance."""
    job = JobRun(
        job_type="test_annotation_job",
        job_function="test_function",
        status=JobStatus.RUNNING,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job
