import pytest

from mavedb.models.job_run import JobRun
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant


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
