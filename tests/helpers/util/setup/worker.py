from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from unittest.mock import patch

from sqlalchemy import select

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
)
from tests.helpers.constants import (
    TEST_CODING_LAYER,
    TEST_GENE_INFO,
    TEST_GENOMIC_LAYER,
    TEST_MAPPING_SCAFFOLD,
    TEST_PROTEIN_LAYER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
)


async def create_variants_in_score_set(
    session, mock_s3_client, score_df, count_df, mock_worker_ctx, variant_creation_run
):
    """Add variants to a given score set in the database."""
    with (
        patch.object(mock_s3_client, "download_fileobj", return_value=None),
        patch(
            "mavedb.worker.jobs.variant_processing.creation.pd.read_csv",
            side_effect=[score_df, count_df],
        ),
    ):
        result = await create_variants_for_score_set(mock_worker_ctx, variant_creation_run.id)

    assert result["status"] == "ok"
    session.commit()


async def create_mappings_in_score_set(
    session, mock_s3_client, mock_worker_ctx, score_df, count_df, variant_creation_run, variant_mapping_run
):
    score_set = await create_variants_in_score_set(
        session, mock_s3_client, score_df, count_df, mock_worker_ctx, variant_creation_run
    )

    async def dummy_mapping_job():
        return await construct_mock_mapping_output(session, score_set, with_layers={"g", "c", "p"})

    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
        patch("mavedb.worker.jobs.variant_processing.mapping.CLIN_GEN_SUBMISSION_ENABLED", False),
    ):
        result = await map_variants_for_score_set(mock_worker_ctx, variant_mapping_run.id)

    assert result["status"] == "ok"
    session.commit()


async def construct_mock_mapping_output(
    session,
    score_set,
    with_layers,
    with_gene_info=True,
    with_pre_mapped=True,
    with_post_mapped=True,
    with_reference_metadata=True,
    with_mapped_scores=True,
    with_all_variants=True,
):
    """Construct mapping output for a given score set in the database."""
    mapping_output = deepcopy(TEST_MAPPING_SCAFFOLD)

    if with_reference_metadata:
        for target in score_set.target_genes:
            mapping_output["reference_sequences"][target.name] = {
                "gene_info": TEST_GENE_INFO if with_gene_info else {},
            }

        for target in score_set.target_genes:
            mapping_output["reference_sequences"][target.name]["layers"] = {}
            if "g" in with_layers:
                mapping_output["reference_sequences"][target.name]["layers"]["g"] = TEST_GENOMIC_LAYER
            if "c" in with_layers:
                mapping_output["reference_sequences"][target.name]["layers"]["c"] = TEST_CODING_LAYER
            if "p" in with_layers:
                mapping_output["reference_sequences"][target.name]["layers"]["p"] = TEST_PROTEIN_LAYER

    if with_mapped_scores:
        variants = session.scalars(
            select(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
        ).all()

        for idx, variant in enumerate(variants):
            mapped_score = {
                "pre_mapped": TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X if with_pre_mapped else {},
                "post_mapped": TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X if with_post_mapped else {},
                "mavedb_id": variant.urn,
            }

            # Skip every other variant if not with_all_variants
            if not with_all_variants and idx % 2 == 0:
                mapped_score["post_mapped"] = {}

            mapping_output["mapped_scores"].append(mapped_score)

    if not mapping_output["mapped_scores"]:
        mapping_output["error_message"] = "test error: no mapped scores"

    return mapping_output
