import pytest

from mavedb.models.enums.job_pipeline import DependencyType
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from tests.helpers.constants import VALID_CAID

try:
    from .conftest_optional import *  # noqa: F403, F401
except ImportError:
    pass


## param fixtures for job runs ##


@pytest.fixture
def create_variants_sample_params(with_populated_domain_data, sample_score_set, sample_user):
    """Provide sample parameters for create_variants_for_score_set job."""

    return {
        "scores_file_key": "sample_scores.csv",
        "counts_file_key": "sample_counts.csv",
        "correlation_id": "sample-correlation-id",
        "updater_id": sample_user.id,
        "score_set_id": sample_score_set.id,
        "score_columns_metadata": {"s_0": {"description": "metadataS", "details": "detailsS"}},
        "count_columns_metadata": {"c_0": {"description": "metadataC", "details": "detailsC"}},
    }


@pytest.fixture
def map_variants_sample_params(with_populated_domain_data, sample_score_set, sample_user):
    """Provide sample parameters for map_variants_for_score_set job."""

    return {
        "score_set_id": sample_score_set.id,
        "correlation_id": "sample-mapping-correlation-id",
        "updater_id": sample_user.id,
    }


@pytest.fixture
def link_gnomad_variants_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for create_variants_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


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
def submit_score_set_mappings_to_car_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for submit_score_set_mappings_to_car job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def refresh_clinvar_controls_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for refresh_clinvar_controls job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
        "month": 1,
        "year": 2026,
    }


## Sample pipeline


@pytest.fixture
def sample_pipeline():
    """Create a sample Pipeline instance for testing."""

    return Pipeline(
        name="Sample Pipeline",
        description="A sample pipeline for testing purposes",
    )


@pytest.fixture
def with_sample_pipeline(session, sample_pipeline):
    """Fixture to ensure sample pipeline exists in the database."""
    session.add(sample_pipeline)
    session.commit()


## Variant creation job fixtures


@pytest.fixture
def dummy_variant_creation_job_run(create_variants_sample_params):
    """Create a dummy variant creation job run for testing."""

    return JobRun(
        urn="test:dummy_variant_creation_job",
        job_type="dummy_variant_creation",
        job_function="dummy_variant_creation_function",
        max_retries=3,
        retry_count=0,
        job_params=create_variants_sample_params,
    )


@pytest.fixture
def dummy_variant_mapping_job_run(map_variants_sample_params):
    """Create a dummy variant mapping job run for testing."""

    return JobRun(
        urn="test:dummy_variant_mapping_job",
        job_type="dummy_variant_mapping",
        job_function="dummy_variant_mapping_function",
        max_retries=3,
        retry_count=0,
        job_params=map_variants_sample_params,
    )


@pytest.fixture
def with_dummy_setup_jobs(
    session,
    dummy_variant_creation_job_run,
    dummy_variant_mapping_job_run,
):
    """Add dummy variant creation and mapping job runs to the session."""

    session.add(dummy_variant_creation_job_run)
    session.add(dummy_variant_mapping_job_run)
    session.commit()


## Gnomad Linkage Job Fixtures ##


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
def setup_sample_variants_with_caid(
    session, with_populated_domain_data, mock_worker_ctx, sample_link_gnomad_variants_run
):
    """Setup variants and mapped variants in the database for testing."""
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
        clingen_allele_id=VALID_CAID,
        current=True,
        mapped_date="2024-01-01T00:00:00Z",
        mapping_api_version="1.0.0",
    )
    session.add(mapped_variant)
    session.commit()
    return variant, mapped_variant


## Uniprot Job Fixtures ##


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


## Clingen Job Fixtures ##


@pytest.fixture
def submit_score_set_mappings_to_car_sample_pipeline():
    """Create a pipeline instance for submit_score_set_mappings_to_car job."""

    return Pipeline(
        urn="test:submit_score_set_mappings_to_car_pipeline",
        name="Submit Score Set Mappings to ClinGen Allele Registry Pipeline",
    )


@pytest.fixture
def submit_score_set_mappings_to_ldh_sample_pipeline():
    """Create a pipeline instance for submit_score_set_mappings_to_ldh job."""

    return Pipeline(
        urn="test:submit_score_set_mappings_to_ldh_pipeline",
        name="Submit Score Set Mappings to ClinGen Allele Registry Pipeline",
    )


@pytest.fixture
def submit_score_set_mappings_to_car_sample_job_run(submit_score_set_mappings_to_car_params):
    """Create a JobRun instance for submit_score_set_mappings_to_car job."""

    return JobRun(
        urn="test:submit_score_set_mappings_to_car",
        job_type="submit_score_set_mappings_to_car",
        job_function="submit_score_set_mappings_to_car",
        max_retries=3,
        retry_count=0,
        job_params=submit_score_set_mappings_to_car_params,
    )


@pytest.fixture
def submit_score_set_mappings_to_ldh_sample_job_run(submit_score_set_mappings_to_car_params):
    """Create a JobRun instance for submit_score_set_mappings_to_car job."""

    return JobRun(
        urn="test:submit_score_set_mappings_to_car",
        job_type="submit_score_set_mappings_to_car",
        job_function="submit_score_set_mappings_to_car",
        max_retries=3,
        retry_count=0,
        job_params=submit_score_set_mappings_to_car_params,
    )


@pytest.fixture
def submit_score_set_mappings_to_car_sample_job_run_in_pipeline(
    session,
    with_submit_score_set_mappings_to_car_pipeline,
    with_submit_score_set_mappings_to_car_job,
    submit_score_set_mappings_to_car_sample_pipeline,
    submit_score_set_mappings_to_car_sample_job_run,
):
    """Provide a context with a submit_score_set_mappings_to_car job run and pipeline."""

    submit_score_set_mappings_to_car_sample_job_run.pipeline_id = submit_score_set_mappings_to_car_sample_pipeline.id
    session.commit()
    return submit_score_set_mappings_to_car_sample_job_run


@pytest.fixture
def submit_score_set_mappings_to_ldh_sample_job_run_in_pipeline(
    session,
    with_submit_score_set_mappings_to_ldh_pipeline,
    with_submit_score_set_mappings_to_ldh_job,
    submit_score_set_mappings_to_ldh_sample_pipeline,
    submit_score_set_mappings_to_ldh_sample_job_run,
):
    """Provide a context with a submit_score_set_mappings_to_ldh job run and pipeline."""

    submit_score_set_mappings_to_ldh_sample_job_run.pipeline_id = submit_score_set_mappings_to_ldh_sample_pipeline.id
    session.commit()
    return submit_score_set_mappings_to_ldh_sample_job_run


@pytest.fixture
def with_submit_score_set_mappings_to_car_job(session, submit_score_set_mappings_to_car_sample_job_run):
    """Add a submit_score_set_mappings_to_car job run to the session."""

    session.add(submit_score_set_mappings_to_car_sample_job_run)
    session.commit()


@pytest.fixture
def with_submit_score_set_mappings_to_ldh_job(session, submit_score_set_mappings_to_ldh_sample_job_run):
    """Add a submit_score_set_mappings_to_ldh job run to the session."""

    session.add(submit_score_set_mappings_to_ldh_sample_job_run)
    session.commit()


@pytest.fixture
def with_submit_score_set_mappings_to_car_pipeline(
    session,
    submit_score_set_mappings_to_car_sample_pipeline,
):
    """Add a submit_score_set_mappings_to_car pipeline to the session."""

    session.add(submit_score_set_mappings_to_car_sample_pipeline)
    session.commit()


@pytest.fixture
def with_submit_score_set_mappings_to_ldh_pipeline(
    session,
    submit_score_set_mappings_to_ldh_sample_pipeline,
):
    """Add a submit_score_set_mappings_to_ldh pipeline to the session."""

    session.add(submit_score_set_mappings_to_ldh_sample_pipeline)
    session.commit()


@pytest.fixture
def sample_independent_variant_creation_run(create_variants_sample_params):
    """Create a JobRun instance for variant creation job."""

    return JobRun(
        urn="test:create_variants_for_score_set",
        job_type="create_variants_for_score_set",
        job_function="create_variants_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=create_variants_sample_params,
    )


@pytest.fixture
def sample_independent_variant_mapping_run(map_variants_sample_params):
    """Create a JobRun instance for variant mapping job."""

    return JobRun(
        urn="test:map_variants_for_score_set",
        job_type="map_variants_for_score_set",
        job_function="map_variants_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=map_variants_sample_params,
    )


@pytest.fixture
def dummy_pipeline_step():
    """Create a dummy pipeline step function for testing."""

    return JobRun(
        urn="test:dummy_pipeline_step",
        job_type="dummy_pipeline_step",
        job_function="dummy_arq_function",
        max_retries=3,
        retry_count=0,
    )


@pytest.fixture
def sample_pipeline_variant_creation_run(
    session,
    with_variant_creation_pipeline,
    sample_variant_creation_pipeline,
    sample_independent_variant_creation_run,
):
    """Create a JobRun instance for variant creation job."""

    sample_independent_variant_creation_run.pipeline_id = sample_variant_creation_pipeline.id
    session.add(sample_independent_variant_creation_run)
    session.commit()
    return sample_independent_variant_creation_run


@pytest.fixture
def sample_pipeline_variant_mapping_run(
    session,
    with_variant_mapping_pipeline,
    sample_independent_variant_mapping_run,
    sample_variant_mapping_pipeline,
):
    """Create a JobRun instance for variant mapping job."""

    sample_independent_variant_mapping_run.pipeline_id = sample_variant_mapping_pipeline.id
    session.add(sample_independent_variant_mapping_run)
    session.commit()
    return sample_independent_variant_mapping_run


@pytest.fixture
def sample_variant_creation_pipeline():
    """Create a Pipeline instance."""

    return Pipeline(
        name="variant_creation_pipeline",
        description="Pipeline for creating variants",
    )


@pytest.fixture
def sample_variant_mapping_pipeline():
    """Create a Pipeline instance."""

    return Pipeline(
        name="variant_mapping_pipeline",
        description="Pipeline for mapping variants",
    )


@pytest.fixture
def with_independent_processing_runs(
    session,
    sample_independent_variant_creation_run,
    sample_independent_variant_mapping_run,
):
    """Fixture to ensure independent variant processing runs exist in the database."""

    session.add(sample_independent_variant_creation_run)
    session.add(sample_independent_variant_mapping_run)
    session.commit()


@pytest.fixture
def with_variant_creation_pipeline(session, sample_variant_creation_pipeline):
    """Fixture to ensure variant creation pipeline and its runs exist in the database."""
    session.add(sample_variant_creation_pipeline)
    session.commit()


@pytest.fixture
def with_variant_creation_pipeline_runs(
    session,
    with_variant_creation_pipeline,
    sample_variant_creation_pipeline,
    sample_pipeline_variant_creation_run,
    dummy_pipeline_step,
):
    """Fixture to ensure pipeline variant processing runs exist in the database."""
    session.add(sample_pipeline_variant_creation_run)
    dummy_pipeline_step.pipeline_id = sample_variant_creation_pipeline.id
    session.add(dummy_pipeline_step)
    session.commit()


@pytest.fixture
def with_variant_mapping_pipeline(session, sample_variant_mapping_pipeline):
    """Fixture to ensure variant mapping pipeline and its runs exist in the database."""
    session.add(sample_variant_mapping_pipeline)
    session.commit()


@pytest.fixture
def with_variant_mapping_pipeline_runs(
    session,
    with_variant_mapping_pipeline,
    sample_variant_mapping_pipeline,
    sample_pipeline_variant_mapping_run,
    dummy_pipeline_step,
):
    """Fixture to ensure pipeline variant processing runs exist in the database."""
    session.add(sample_pipeline_variant_mapping_run)
    dummy_pipeline_step.pipeline_id = sample_variant_mapping_pipeline.id
    session.add(dummy_pipeline_step)
    session.commit()


@pytest.fixture
def sample_dummy_pipeline():
    """Create a sample Pipeline instance for testing."""

    return Pipeline(
        name="Dummy Pipeline",
        description="A dummy pipeline for testing purposes",
    )


@pytest.fixture
def with_dummy_pipeline(session, sample_dummy_pipeline):
    """Fixture to ensure dummy pipeline exists in the database."""
    session.add(sample_dummy_pipeline)
    session.commit()


@pytest.fixture
def sample_dummy_pipeline_start(session, with_dummy_pipeline, sample_dummy_pipeline):
    """Create a sample JobRun instance for starting the dummy pipeline."""
    start_job_run = JobRun(
        pipeline_id=sample_dummy_pipeline.id,
        job_type="start_pipeline",
        job_function="start_pipeline",
    )
    session.add(start_job_run)
    session.commit()

    return start_job_run


@pytest.fixture
def with_dummy_pipeline_start(session, with_dummy_pipeline, sample_dummy_pipeline_start):
    """Fixture to ensure a start pipeline job run for the dummy pipeline exists in the database."""
    session.add(sample_dummy_pipeline_start)
    session.commit()


@pytest.fixture
def sample_dummy_pipeline_step(session, sample_dummy_pipeline):
    """Create a sample PipelineStep instance for the dummy pipeline."""
    step = JobRun(
        pipeline_id=sample_dummy_pipeline.id,
        job_type="dummy_step",
        job_function="dummy_arq_function",
    )
    session.add(step)
    session.commit()
    return step


@pytest.fixture
def with_full_dummy_pipeline(session, with_dummy_pipeline_start, sample_dummy_pipeline, sample_dummy_pipeline_step):
    """Fixture to ensure dummy pipeline steps exist in the database."""
    session.add(sample_dummy_pipeline_step)
    session.commit()


@pytest.fixture
def sample_refresh_clinvar_controls_job_run(refresh_clinvar_controls_sample_params):
    """Create a JobRun instance for refresh_clinvar_controls job."""

    return JobRun(
        urn="test:refresh_clinvar_controls",
        job_type="refresh_clinvar_controls",
        job_function="refresh_clinvar_controls",
        max_retries=3,
        retry_count=0,
        job_params=refresh_clinvar_controls_sample_params,
    )


@pytest.fixture
def with_refresh_clinvar_controls_job(session, sample_refresh_clinvar_controls_job_run):
    """Add a refresh_clinvar_controls job run to the session."""

    session.add(sample_refresh_clinvar_controls_job_run)
    session.commit()


@pytest.fixture
def sample_refresh_clinvar_controls_pipeline():
    """Create a pipeline instance for refresh_clinvar_controls job."""

    return Pipeline(
        urn="test:refresh_clinvar_controls_pipeline",
        name="Refresh ClinVar Controls Pipeline",
    )


@pytest.fixture
def with_refresh_clinvar_controls_pipeline(
    session,
    sample_refresh_clinvar_controls_pipeline,
):
    """Add a refresh_clinvar_controls pipeline to the session."""

    session.add(sample_refresh_clinvar_controls_pipeline)
    session.commit()


@pytest.fixture
def sample_refresh_clinvar_controls_job_in_pipeline(
    session,
    with_refresh_clinvar_controls_job,
    with_refresh_clinvar_controls_pipeline,
    sample_refresh_clinvar_controls_job_run,
    sample_refresh_clinvar_controls_pipeline,
):
    """Provide a context with a refresh_clinvar_controls job run and pipeline."""

    sample_refresh_clinvar_controls_job_run.pipeline_id = sample_refresh_clinvar_controls_pipeline.id
    session.commit()
    return sample_refresh_clinvar_controls_job_run
