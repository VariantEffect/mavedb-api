import pytest
from sqlalchemy import select

from mavedb.models.allele import Allele
from mavedb.models.enums.job_pipeline import DependencyType
from mavedb.models.job_dependency import JobDependency
from mavedb.models.job_run import JobRun
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
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
def reverse_translate_variants_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for reverse_translate_variants_for_score_set job."""

    return {
        "score_set_id": sample_score_set.id,
        "correlation_id": "sample-reverse-translation-correlation-id",
    }


@pytest.fixture
def sample_independent_reverse_translation_run(reverse_translate_variants_sample_params):
    """Create a JobRun instance for the reverse_translate_variants_for_score_set job."""

    return JobRun(
        urn="test:reverse_translate_variants_for_score_set",
        job_type="reverse_translate_variants_for_score_set",
        job_function="reverse_translate_variants_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=reverse_translate_variants_sample_params,
    )


@pytest.fixture
def with_reverse_translation_run(session, sample_independent_reverse_translation_run):
    """Add a reverse_translate_variants_for_score_set job run to the session."""

    session.add(sample_independent_reverse_translation_run)
    session.commit()


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


@pytest.fixture
def setup_sample_alleles_with_caid(session, with_populated_domain_data, sample_link_gnomad_variants_run):
    """Set up new-model rows (Variant + live MappingRecord + authoritative MappingRecordAllele + Allele)
    for the gnomAD linkage job. The allele carries the CAID matched by the mocked Athena row, and the
    allele is the authoritative measurement for the variant so the bandaid seam writes its VAS row.
    """
    score_set = session.get(ScoreSet, sample_link_gnomad_variants_run.job_params["score_set_id"])

    variant = Variant(
        urn="urn:variant:test-variant-with-allele-caid",
        score_set_id=score_set.id,
        hgvs_nt="NM_000000.1:c.1A>G",
        hgvs_pro="NP_000000.1:p.Met1Val",
        data={"hgvs_c": "NM_000000.1:c.1A>G", "hgvs_p": "NP_000000.1:p.Met1Val"},
    )
    allele = Allele(
        vrs_digest="test-allele-vrs-digest",
        level="genomic",
        clingen_allele_id=VALID_CAID,
        post_mapped={"type": "Allele", "expressions": [{"value": "NM_000000.1:c.1A>G", "syntax": "hgvs.c"}]},
    )
    session.add_all([variant, allele])
    session.commit()

    mapping_record = MappingRecord(
        variant_id=variant.id,
        assay_level="genomic",
        mapping_api_version="pytest.0.0",
    )
    session.add(mapping_record)
    session.commit()

    session.add(
        MappingRecordAllele(
            mapping_record_id=mapping_record.id,
            allele_id=allele.id,
            is_authoritative=True,
        )
    )
    session.commit()
    return variant, allele


@pytest.fixture
def setup_rt_derived_allele_with_caid(session, setup_sample_alleles_with_caid):
    """Add a NON-authoritative (RT-derived) allele to the variant's current mapping record, carrying
    the CAID the mocked gnomAD row matches. The authoritative allele is given a CAID with no gnomAD
    match, so only the RT-derived allele can link. This isolates the requirement that gnomAD linkage
    must cover the full allele set (authoritative + RT-derived), not just authoritative links — for
    protein/coding score sets the genomic allele gnomAD knows is the RT-derived one.
    """
    variant, authoritative_allele = setup_sample_alleles_with_caid

    # Authoritative allele's CAID intentionally has no gnomAD match, so it cannot be what links.
    authoritative_allele.clingen_allele_id = "CA_NO_GNOMAD_MATCH"
    session.add(authoritative_allele)

    mapping_record = session.scalars(
        select(MappingRecord).where(MappingRecord.variant_id == variant.id, MappingRecord.current)
    ).one()

    rt_allele = Allele(
        vrs_digest="test-rt-derived-allele-vrs-digest",
        level="genomic",
        clingen_allele_id=VALID_CAID,
        post_mapped={"type": "Allele", "expressions": [{"value": "NC_000001.11:g.12345G>A", "syntax": "hgvs.g"}]},
    )
    session.add(rt_allele)
    session.commit()

    session.add(
        MappingRecordAllele(
            mapping_record_id=mapping_record.id,
            allele_id=rt_allele.id,
            is_authoritative=False,
        )
    )
    session.commit()
    return variant, authoritative_allele, rt_allele


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


## Janitor job fixtures


@pytest.fixture
def sample_cleanup_job_run():
    """Create a JobRun instance for a cleanup job."""

    return JobRun(
        urn="test:cleanup_job",
        job_type="cleanup_job",
        job_function="cleanup_function",
        max_retries=3,
        retry_count=0,
    )


@pytest.fixture
def with_cleanup_job(session, sample_cleanup_job_run):
    """Add a cleanup job run to the session."""

    session.add(sample_cleanup_job_run)
    session.commit()


## HGVS Population Job Fixtures ##


@pytest.fixture
def populate_hgvs_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for populate_hgvs_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def sample_populate_hgvs_pipeline():
    """Create a pipeline instance for populate_hgvs_for_score_set job."""

    return Pipeline(
        urn="test:populate_hgvs_pipeline",
        name="Populate HGVS Pipeline",
    )


@pytest.fixture
def sample_populate_hgvs_run(populate_hgvs_sample_params):
    """Create a JobRun instance for populate_hgvs_for_score_set job."""

    return JobRun(
        urn="test:populate_hgvs_for_score_set",
        job_type="populate_hgvs_for_score_set",
        job_function="populate_hgvs_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=populate_hgvs_sample_params,
    )


@pytest.fixture
def with_populate_hgvs_job(session, sample_populate_hgvs_run):
    """Add a populate_hgvs_for_score_set job run to the session."""

    session.add(sample_populate_hgvs_run)
    session.commit()


@pytest.fixture
def with_populate_hgvs_pipeline(session, sample_populate_hgvs_pipeline):
    """Add a populate_hgvs pipeline to the session."""

    session.add(sample_populate_hgvs_pipeline)
    session.commit()


@pytest.fixture
def sample_populate_hgvs_run_pipeline(
    session,
    with_populate_hgvs_job,
    with_populate_hgvs_pipeline,
    sample_populate_hgvs_run,
    sample_populate_hgvs_pipeline,
):
    """Provide a context with a populate_hgvs job run and pipeline."""

    sample_populate_hgvs_run.pipeline_id = sample_populate_hgvs_pipeline.id
    session.commit()
    return sample_populate_hgvs_run


@pytest.fixture
def setup_sample_variants_with_caid_for_hgvs(
    session, with_populated_domain_data, mock_worker_ctx, sample_populate_hgvs_run
):
    """Setup variants and mapped variants in the database for HGVS population testing."""
    score_set = session.get(ScoreSet, sample_populate_hgvs_run.job_params["score_set_id"])

    variant = Variant(
        urn="urn:variant:test-variant-with-caid-hgvs",
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


# --- Variant Translation Fixtures ---


@pytest.fixture
def populate_variant_translations_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for populate_variant_translations_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def sample_populate_variant_translations_pipeline():
    """Create a pipeline instance for populate_variant_translations_for_score_set job."""

    return Pipeline(
        urn="test:populate_variant_translations_pipeline",
        name="Populate Variant Translations Pipeline",
    )


@pytest.fixture
def sample_populate_variant_translations_run(populate_variant_translations_sample_params):
    """Create a JobRun instance for populate_variant_translations_for_score_set job."""

    return JobRun(
        urn="test:populate_variant_translations_for_score_set",
        job_type="populate_variant_translations_for_score_set",
        job_function="populate_variant_translations_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=populate_variant_translations_sample_params,
    )


@pytest.fixture
def with_populate_variant_translations_job(session, sample_populate_variant_translations_run):
    """Add a populate_variant_translations_for_score_set job run to the session."""

    session.add(sample_populate_variant_translations_run)
    session.commit()


@pytest.fixture
def with_populate_variant_translations_pipeline(session, sample_populate_variant_translations_pipeline):
    """Add a populate_variant_translations pipeline to the session."""

    session.add(sample_populate_variant_translations_pipeline)
    session.commit()


@pytest.fixture
def sample_populate_variant_translations_run_pipeline(
    session,
    with_populate_variant_translations_job,
    with_populate_variant_translations_pipeline,
    sample_populate_variant_translations_run,
    sample_populate_variant_translations_pipeline,
):
    """Provide a context with a populate_variant_translations job run and pipeline."""

    sample_populate_variant_translations_run.pipeline_id = sample_populate_variant_translations_pipeline.id
    session.commit()
    return sample_populate_variant_translations_run


@pytest.fixture
def setup_sample_variants_with_caid_for_translation(
    session, with_populated_domain_data, mock_worker_ctx, sample_populate_variant_translations_run
):
    """Setup variants and mapped variants in the database for variant translation testing."""
    score_set = session.get(ScoreSet, sample_populate_variant_translations_run.job_params["score_set_id"])

    variant = Variant(
        urn="urn:variant:test-variant-with-caid-translation",
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


## ClinGen Cache Warming Job Fixtures ##


@pytest.fixture
def warm_clingen_cache_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for warm_clingen_cache job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def sample_warm_clingen_cache_job_run(warm_clingen_cache_sample_params):
    """Create a JobRun instance for warm_clingen_cache job."""

    return JobRun(
        urn="test:warm_clingen_cache",
        job_type="warm_clingen_cache",
        job_function="warm_clingen_cache",
        max_retries=3,
        retry_count=0,
        job_params=warm_clingen_cache_sample_params,
    )


@pytest.fixture
def with_warm_clingen_cache_job(session, sample_warm_clingen_cache_job_run):
    """Add a warm_clingen_cache job run to the session."""

    session.add(sample_warm_clingen_cache_job_run)
    session.commit()


@pytest.fixture
def sample_warm_clingen_cache_pipeline():
    """Create a pipeline instance for warm_clingen_cache job."""

    return Pipeline(
        urn="test:warm_clingen_cache_pipeline",
        name="Warm ClinGen Cache Pipeline",
    )


@pytest.fixture
def with_warm_clingen_cache_pipeline(session, sample_warm_clingen_cache_pipeline):
    """Add a warm_clingen_cache pipeline to the session."""

    session.add(sample_warm_clingen_cache_pipeline)
    session.commit()


@pytest.fixture
def sample_warm_clingen_cache_job_in_pipeline(
    session,
    with_warm_clingen_cache_job,
    with_warm_clingen_cache_pipeline,
    sample_warm_clingen_cache_job_run,
    sample_warm_clingen_cache_pipeline,
):
    """Provide a context with a warm_clingen_cache job run and pipeline."""

    sample_warm_clingen_cache_job_run.pipeline_id = sample_warm_clingen_cache_pipeline.id
    session.commit()
    return sample_warm_clingen_cache_job_run


## VEP Population Job Fixtures ##


@pytest.fixture
def populate_vep_sample_params(with_populated_domain_data, sample_score_set):
    """Provide sample parameters for populate_vep_for_score_set job."""

    return {
        "correlation_id": "sample-correlation-id",
        "score_set_id": sample_score_set.id,
    }


@pytest.fixture
def sample_populate_vep_pipeline():
    """Create a pipeline instance for populate_vep_for_score_set job."""

    return Pipeline(
        urn="test:populate_vep_pipeline",
        name="Populate VEP Pipeline",
    )


@pytest.fixture
def sample_populate_vep_run(populate_vep_sample_params):
    """Create a JobRun instance for populate_vep_for_score_set job."""

    return JobRun(
        urn="test:populate_vep_for_score_set",
        job_type="populate_vep_for_score_set",
        job_function="populate_vep_for_score_set",
        max_retries=3,
        retry_count=0,
        job_params=populate_vep_sample_params,
    )


@pytest.fixture
def with_populate_vep_job(session, sample_populate_vep_run):
    """Add a populate_vep_for_score_set job run to the session."""

    session.add(sample_populate_vep_run)
    session.commit()


@pytest.fixture
def with_populate_vep_pipeline(session, sample_populate_vep_pipeline):
    """Add a populate_vep pipeline to the session."""

    session.add(sample_populate_vep_pipeline)
    session.commit()


@pytest.fixture
def sample_populate_vep_run_pipeline(
    session,
    with_populate_vep_job,
    with_populate_vep_pipeline,
    sample_populate_vep_run,
    sample_populate_vep_pipeline,
):
    """Provide a context with a populate_vep job run and pipeline."""

    sample_populate_vep_run.pipeline_id = sample_populate_vep_pipeline.id
    session.commit()
    return sample_populate_vep_run


@pytest.fixture
def setup_sample_variants_for_vep(session, with_populated_domain_data, mock_worker_ctx, sample_populate_vep_run):
    """Setup a variant and mapped variant with hgvs_assay_level for VEP testing."""
    score_set = session.get(ScoreSet, sample_populate_vep_run.job_params["score_set_id"])

    variant = Variant(
        urn="urn:variant:test-variant-for-vep",
        score_set_id=score_set.id,
        hgvs_nt="NM_007294.4:c.5A>G",
        hgvs_pro="NP_009225.1:p.Cys2Tyr",
        data={"hgvs_c": "NM_007294.4:c.5A>G", "hgvs_p": "NP_009225.1:p.Cys2Tyr"},
    )
    session.add(variant)
    session.commit()
    mapped_variant = MappedVariant(
        variant_id=variant.id,
        current=True,
        mapped_date="2024-01-01T00:00:00Z",
        mapping_api_version="1.0.0",
        post_mapped={"type": "Allele", "expressions": [{"value": "NM_007294.4:c.5A>G", "syntax": "hgvs.c"}]},
        hgvs_assay_level="NM_007294.4:c.5A>G",
    )
    session.add(mapped_variant)
    session.commit()
    return variant, mapped_variant


@pytest.fixture
def setup_sample_protein_variant_for_vep(session, with_populated_domain_data, mock_worker_ctx, sample_populate_vep_run):
    """Setup a protein HGVS variant (NP_ accession) that VEP cannot resolve directly.

    VEP's /vep/human/hgvs endpoint does not return results for protein HGVS strings like
    NP_009225.1:p.Val1696His, so these must be recoded via Variant Recoder first.  This fixture
    exercises the recoder fallback path end-to-end.
    """
    score_set = session.get(ScoreSet, sample_populate_vep_run.job_params["score_set_id"])

    variant = Variant(
        urn="urn:variant:test-protein-variant-for-vep",
        score_set_id=score_set.id,
        hgvs_pro="NP_009225.1:p.Val1696His",
        data={"hgvs_p": "NP_009225.1:p.Val1696His"},
    )
    session.add(variant)
    session.commit()

    mapped_variant = MappedVariant(
        variant_id=variant.id,
        current=True,
        mapped_date="2024-01-01T00:00:00Z",
        mapping_api_version="1.0.0",
        post_mapped={"type": "Allele", "expressions": [{"value": "NP_009225.1:p.Val1696His", "syntax": "hgvs.p"}]},
        hgvs_assay_level="NP_009225.1:p.Val1696His",
    )
    session.add(mapped_variant)
    session.commit()
    return variant, mapped_variant
