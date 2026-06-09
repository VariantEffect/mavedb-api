from asyncio.unix_events import _UnixSelectorEventLoop
from copy import deepcopy
from unittest.mock import patch

from sqlalchemy import select

from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import (
    TEST_CODING_LAYER,
    TEST_GENE_INFO,
    TEST_GENOMIC_LAYER,
    TEST_MAPPING_SCAFFOLD,
    TEST_PROTEIN_LAYER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
)


# Placeholder reference accessions for qualifying target-relative HGVS in mock
# post-mapped output, keyed by the HGVS kind prefix (``c.``/``n.``/``g.``/``p.``).
_PLACEHOLDER_ACCESSIONS = {
    "c.": "NM_999999.1",
    "n.": "NR_999999.1",
    "g.": "NC_999999.1",
    "p.": "NP_999999.1",
}


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
        # Guard against both possible function signatures, with some uses of this function coming from
        # integration tests that need not pass a JobManager.
        try:
            result = await create_variants_for_score_set(
                mock_worker_ctx,
                variant_creation_run.id,
            )
        except TypeError:
            result = await create_variants_for_score_set(
                mock_worker_ctx,
                variant_creation_run.id,
                JobManager(session, mock_worker_ctx["redis"], variant_creation_run.id),
            )

    assert result.status == JobStatus.SUCCEEDED
    session.commit()


async def create_mappings_in_score_set(
    session, mock_s3_client, mock_worker_ctx, score_df, count_df, variant_creation_run, variant_mapping_run
):
    await create_variants_in_score_set(
        session, mock_s3_client, score_df, count_df, mock_worker_ctx, variant_creation_run
    )

    score_set = session.execute(
        select(ScoreSetDbModel).where(ScoreSetDbModel.id == variant_creation_run.job_params["score_set_id"])
    ).scalar_one()

    async def dummy_mapping_job():
        return await construct_mock_mapping_output(session, score_set, with_layers={"g", "c", "p"})

    with (
        patch.object(
            _UnixSelectorEventLoop,
            "run_in_executor",
            return_value=dummy_mapping_job(),
        ),
    ):
        # Guard against both possible function signatures, with some uses of this function coming from
        # integration tests that need not pass a JobManager.
        try:
            result = await map_variants_for_score_set(mock_worker_ctx, variant_mapping_run.id)
        except TypeError:
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], variant_mapping_run.id),
            )

    assert result.status == JobStatus.SUCCEEDED
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

        # Mirror the dcd-mapping QC API: emit one TargetMapping per (target × layer).
        # Tests don't exercise QC numerics, so we only populate the fields the worker
        # needs (target_gene_identifier, alignment_level, tool_version, preferred).
        layers_emitted = [layer for layer in ("g", "c", "p") if layer in with_layers]
        for target in score_set.target_genes:
            for idx, layer in enumerate(layers_emitted):
                mapping_output["target_mappings"].append(
                    {
                        "target_gene_identifier": target.name,
                        "alignment_level": layer,
                        "preferred": idx == 0,
                        # The accession this level was aligned against — a transcript (NM_)
                        # only at the coding level. Reverse translation resolves its coding
                        # transcript hint from the cdna entry's reference_accession.
                        "reference_accession": _PLACEHOLDER_ACCESSIONS[f"{layer}."],
                        "tool_name": "dcd-mapping",
                        "tool_version": "pytest.0.0",
                        "tool_parameters": {},
                        "alignment_metadata": {},
                        "vrs_version": "2",
                    }
                )

    if with_mapped_scores:
        variants = session.scalars(
            select(Variant).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set.urn)
        ).all()

        # Pick a deterministic target / alignment_level to attribute every mapped score
        # to; this matches dcd-mapping's preferred_layer_only behavior in tests.
        default_target_name = score_set.target_genes[0].name if score_set.target_genes else None
        default_alignment_level = next((layer for layer in ("g", "c", "p") if layer in with_layers), None)

        for idx, variant in enumerate(variants):
            mapped_score = {
                "pre_mapped": deepcopy(TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X) if with_pre_mapped else {},
                "post_mapped": deepcopy(TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X) if with_post_mapped else {},
                "mavedb_id": variant.urn,
                "target_gene_identifier": default_target_name,
                "alignment_level": default_alignment_level,
            }

            # Reuse the variant's own HGVS in post-mapped output to keep assertions simple.
            # Real mapper post-mapped HGVS is always accession-qualified (it is mapped onto
            # a reference sequence), so prefix a placeholder accession when the source HGVS
            # is target-relative (a bare ``c.``/``n.``/``g.``/``p.`` with no accession).
            if with_post_mapped:
                hgvs = variant.hgvs_nt or variant.hgvs_pro
                if hgvs and ":" not in hgvs:
                    hgvs = f"{_PLACEHOLDER_ACCESSIONS.get(hgvs[:2], 'NM_999999.1')}:{hgvs}"
                mapped_score["post_mapped"]["expressions"][0]["value"] = hgvs

            # Skip every other variant if not with_all_variants
            if not with_all_variants and idx % 2 == 0:
                mapped_score["post_mapped"] = {}

            # Mirror the mapper's per-record outcome: both alleles present -> MAPPED, else
            # FAILED. Tests needing benign outcomes set ``outcome`` explicitly afterward.
            mapped_score["outcome"] = (
                "mapped" if mapped_score["pre_mapped"] and mapped_score["post_mapped"] else "failed"
            )

            mapping_output["mapped_scores"].append(mapped_score)

    if not mapping_output["mapped_scores"]:
        mapping_output["error_message"] = "test error: no mapped scores"

    return mapping_output
