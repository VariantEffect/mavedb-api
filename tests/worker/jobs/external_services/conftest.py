import pytest

from mavedb.models.enums.job_pipeline import DependencyType
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

## Gnomad Linkage Job Fixtures ##


@pytest.fixture
def link_gnomad_variants_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for create_variants_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def sample_link_gnomad_variants_pipeline():
    """Create a pipeline instance for link_gnomad_variants job."""

    return Pipeline(
        urn="test:link_gnomad_variants_pipeline",
        name="Link gnomAD Variants Pipeline",
    )


@pytest.fixture
def sample_link_gnomad_variants_run(link_gnomad_variants_sample_params):
    """Create a JobRun instance for link_gnomad_variants job."""

    return JobRun(
        urn="test:link_gnomad_variants",
        job_type="link_gnomad_variants",
        job_function="link_gnomad_variants",
        max_retries=3,
        retry_count=0,
        job_params=link_gnomad_variants_sample_params,
    )


@pytest.fixture
def with_gnomad_linking_job(session, sample_link_gnomad_variants_run):
    """Add a link_gnomad_variants job run to the session."""

    session.add(sample_link_gnomad_variants_run)
    session.commit()


@pytest.fixture
def with_gnomad_linking_pipeline(session, sample_link_gnomad_variants_pipeline):
    """Add a link_gnomad_variants pipeline to the session."""

    session.add(sample_link_gnomad_variants_pipeline)
    session.commit()


@pytest.fixture
def sample_link_gnomad_variants_run_pipeline(
    session,
    with_gnomad_linking_job,
    with_gnomad_linking_pipeline,
    sample_link_gnomad_variants_run,
    sample_link_gnomad_variants_pipeline,
):
    """Provide a context with a link_gnomad_variants job run and pipeline."""

    sample_link_gnomad_variants_run.pipeline_id = sample_link_gnomad_variants_pipeline.id
    session.commit()
    return sample_link_gnomad_variants_run


@pytest.fixture
def setup_sample_variants_with_caid(with_populated_domain_data, mock_worker_ctx, sample_link_gnomad_variants_run):
    """Setup variants and mapped variants in the database for testing."""
    session = mock_worker_ctx["db"]
    score_set = session.get(ScoreSet, sample_link_gnomad_variants_run.job_params["score_set_id"])

    # Add a variant and mapped variant to the database with a CAID
    variant = Variant(
        urn="urn:variant:test-variant-with-caid",
        score_set_id=score_set.id,
        hgvs_nt="NM_000000.1:c.1A>G",
        hgvs_pro="NP_000000.1:p.Met1Val",
        data={"hgvs_c": "NM_000000.1:c.1A>G", "hgvs_p": "NP_000000.1:p.Met1Val"},
    )
    session.add(variant)
    session.commit()
    mapped_variant = MappedVariant(
        variant_id=variant.id,
        clingen_allele_id="CA123",
        current=True,
        mapped_date="2024-01-01T00:00:00Z",
        mapping_api_version="1.0.0",
    )
    session.add(mapped_variant)
    session.commit()


## Uniprot Job Fixtures ##


@pytest.fixture
def submit_uniprot_mapping_jobs_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for submit_uniprot_mapping_jobs_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def poll_uniprot_mapping_jobs_sample_params(
    submit_uniprot_mapping_jobs_sample_params,
    with_dependent_polling_job_for_submission_run,
):
    """Provide sample parameters for poll_uniprot_mapping_jobs_for_score_set job."""

    return {
        "correlation_id": submit_uniprot_mapping_jobs_sample_params["correlation_id"],
        "score_set_id": submit_uniprot_mapping_jobs_sample_params["score_set_id"],
        "mapping_jobs": {},
    }


@pytest.fixture
def sample_submit_uniprot_mapping_jobs_pipeline():
    """Create a pipeline instance for submit_uniprot_mapping_jobs_for_score_set job."""

    return Pipeline(
        urn="test:submit_uniprot_mapping_jobs_pipeline",
        name="Submit UniProt Mapping Jobs Pipeline",
    )


@pytest.fixture
def sample_poll_uniprot_mapping_jobs_pipeline():
    """Create a pipeline instance for poll_uniprot_mapping_jobs_for_score_set job."""

    return Pipeline(
        urn="test:poll_uniprot_mapping_jobs_pipeline",
        name="Poll UniProt Mapping Jobs Pipeline",
    )


@pytest.fixture
def sample_submit_uniprot_mapping_jobs_run(submit_uniprot_mapping_jobs_sample_params):
    """Create a JobRun instance for submit_uniprot_mapping_jobs_for_score_set job."""

    return JobRun(
        urn="test:submit_uniprot_mapping_jobs",
        job_type="submit_uniprot_mapping_jobs",
        job_function="submit_uniprot_mapping_jobs_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=submit_uniprot_mapping_jobs_sample_params,
    )


@pytest.fixture
def sample_dummy_polling_job_for_submission_run(
    session,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_run,
):
    """Create a sample dummy dependent polling job for the submission run."""

    dependent_job = JobRun(
        urn="test:dummy_poll_uniprot_mapping_jobs",
        job_type="dummy_poll_uniprot_mapping_jobs",
        job_function="dummy_arq_function",
        max_retries=3,
        retry_count=0,
        job_params={
            "correlation_id": sample_submit_uniprot_mapping_jobs_run.job_params["correlation_id"],
            "score_set_id": sample_submit_uniprot_mapping_jobs_run.job_params["score_set_id"],
            "mapping_jobs": {},
        },
    )

    return dependent_job


@pytest.fixture
def sample_polling_job_for_submission_run(
    session,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_run,
):
    """Create a sample dependent polling job for the submission run."""

    dependent_job = JobRun(
        urn="test:dependent_poll_uniprot_mapping_jobs",
        job_type="dependent_poll_uniprot_mapping_jobs",
        job_function="poll_uniprot_mapping_jobs_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params={
            "correlation_id": sample_submit_uniprot_mapping_jobs_run.job_params["correlation_id"],
            "score_set_id": sample_submit_uniprot_mapping_jobs_run.job_params["score_set_id"],
            "mapping_jobs": {},
        },
    )

    return dependent_job


@pytest.fixture
def with_dummy_polling_job_for_submission_run(
    session,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_run,
    sample_dummy_polling_job_for_submission_run,
):
    """Create a sample dummy dependent polling job for the submission run."""
    session.add(sample_dummy_polling_job_for_submission_run)
    session.commit()

    dependency = JobDependency(
        id=sample_dummy_polling_job_for_submission_run.id,
        depends_on_job_id=sample_submit_uniprot_mapping_jobs_run.id,
        dependency_type=DependencyType.SUCCESS_REQUIRED,
    )
    session.add(dependency)
    session.commit()


@pytest.fixture
def with_dependent_polling_job_for_submission_run(
    session,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_run,
    sample_polling_job_for_submission_run,
):
    """Create a sample dependent polling job for the submission run."""
    session.add(sample_polling_job_for_submission_run)
    session.commit()

    dependency = JobDependency(
        id=sample_polling_job_for_submission_run.id,
        depends_on_job_id=sample_submit_uniprot_mapping_jobs_run.id,
        dependency_type=DependencyType.SUCCESS_REQUIRED,
    )
    session.add(dependency)
    session.commit()


@pytest.fixture
def with_independent_polling_job_for_submission_run(
    session,
    sample_polling_job_for_submission_run,
):
    """Create a sample dependent polling job for the submission run."""
    session.add(sample_polling_job_for_submission_run)
    session.commit()


@pytest.fixture
def with_submit_uniprot_mapping_job(session, sample_submit_uniprot_mapping_jobs_run):
    """Add a submit_uniprot_mapping_jobs job run to the session."""

    session.add(sample_submit_uniprot_mapping_jobs_run)
    session.commit()


@pytest.fixture
def with_poll_uniprot_mapping_job(session, sample_poll_uniprot_mapping_jobs_run):
    """Add a poll_uniprot_mapping_jobs job run to the session."""

    session.add(sample_poll_uniprot_mapping_jobs_run)
    session.commit()


@pytest.fixture
def sample_submit_uniprot_mapping_jobs_run_in_pipeline(
    session,
    with_submit_uniprot_mapping_job,
    with_submit_uniprot_mapping_jobs_pipeline,
    sample_submit_uniprot_mapping_jobs_run,
    sample_submit_uniprot_mapping_jobs_pipeline,
):
    """Provide a context with a submit_uniprot_mapping_jobs job run and pipeline."""

    sample_submit_uniprot_mapping_jobs_run.pipeline_id = sample_submit_uniprot_mapping_jobs_pipeline.id
    session.commit()
    return sample_submit_uniprot_mapping_jobs_run


@pytest.fixture
def sample_poll_uniprot_mapping_jobs_run_in_pipeline(
    session,
    with_independent_polling_job_for_submission_run,
    with_poll_uniprot_mapping_jobs_pipeline,
    sample_polling_job_for_submission_run,
    sample_poll_uniprot_mapping_jobs_pipeline,
):
    """Provide a context with a poll_uniprot_mapping_jobs job run and pipeline."""

    sample_polling_job_for_submission_run.pipeline_id = sample_poll_uniprot_mapping_jobs_pipeline.id
    session.commit()
    return sample_polling_job_for_submission_run


@pytest.fixture
def sample_dummy_polling_job_for_submission_run_in_pipeline(
    session,
    with_dummy_polling_job_for_submission_run,
    with_submit_uniprot_mapping_jobs_pipeline,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_pipeline,
    sample_submit_uniprot_mapping_jobs_run_in_pipeline,
    sample_dummy_polling_job_for_submission_run,
):
    """Provide a context with a dependent polling job run in the pipeline."""

    dependent_job = sample_dummy_polling_job_for_submission_run
    dependent_job.pipeline_id = sample_submit_uniprot_mapping_jobs_pipeline.id
    session.commit()
    return dependent_job


@pytest.fixture
def sample_polling_job_for_submission_run_in_pipeline(
    session,
    with_dependent_polling_job_for_submission_run,
    with_submit_uniprot_mapping_jobs_pipeline,
    with_submit_uniprot_mapping_job,
    sample_submit_uniprot_mapping_jobs_pipeline,
    sample_submit_uniprot_mapping_jobs_run_in_pipeline,
    sample_polling_job_for_submission_run,
):
    """Provide a context with a dependent polling job run in the pipeline."""

    dependent_job = sample_polling_job_for_submission_run
    dependent_job.pipeline_id = sample_submit_uniprot_mapping_jobs_pipeline.id
    session.commit()
    return dependent_job


@pytest.fixture
def with_submit_uniprot_mapping_jobs_pipeline(
    session,
    sample_submit_uniprot_mapping_jobs_pipeline,
):
    """Add a submit_uniprot_mapping_jobs pipeline to the session."""

    session.add(sample_submit_uniprot_mapping_jobs_pipeline)
    session.commit()


@pytest.fixture
def with_poll_uniprot_mapping_jobs_pipeline(
    session,
    sample_poll_uniprot_mapping_jobs_pipeline,
):
    """Add a poll_uniprot_mapping_jobs pipeline to the session."""
    session.add(sample_poll_uniprot_mapping_jobs_pipeline)
    session.commit()
