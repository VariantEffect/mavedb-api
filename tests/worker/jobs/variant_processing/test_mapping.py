# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from asyncio.unix_events import _UnixSelectorEventLoop
from datetime import date
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import NoResultFound

from mavedb.lib.mapping import EXCLUDED_PREMAPPED_ANNOTATION_KEYS
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.variant_processing.mapping import map_variants_for_score_set
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import TEST_CODING_LAYER, TEST_GENOMIC_LAYER, TEST_PROTEIN_LAYER
from tests.helpers.util.setup.worker import construct_mock_mapping_output, create_variants_in_score_set

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


def authoritative_allele_for(session, mapping_record):
    """Return the authoritative (post-mapped) Allele linked to a mapping record, or None.

    Under the allele data model the post-mapped VRS representation lives on an
    ``Allele`` joined to the ``MappingRecord`` through ``MappingRecordAllele``.
    A successfully post-mapped variant has exactly one authoritative link; a
    variant that failed to post-map has a mapping record but no such link.
    """
    return (
        session.query(Allele)
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .filter(
            MappingRecordAllele.mapping_record_id == mapping_record.id,
            MappingRecordAllele.is_authoritative.is_(True),
        )
        .one_or_none()
    )


@pytest.mark.unit
@pytest.mark.asyncio
class TestMapVariantsForScoreSetUnit:
    """Unit tests for map_variants_for_score_set job."""

    async def dummy_mapping_output(self, output_data={}):
        return output_data

    async def test_map_variants_for_score_set_no_mapping_results(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no mapping results are found."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=self.dummy_mapping_output({})),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED
        assert "score_set_id" in result.data

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Mapping results were not returned from VRS mapping service"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify no annotations were created
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 0

    async def test_map_variants_for_score_set_no_mapped_scores(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no scores are mapped."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {
                        "mapped_scores": [],
                        "error_message": "No variants were mapped for this score set",
                    }
                ),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED
        assert "score_set_id" in result.data

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "No variants were mapped for this score set" in sample_score_set.mapping_errors["error_message"]

        # Verify no annotations were created
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 0

    async def test_map_variants_for_score_set_no_reference_data(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no reference data is available."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {
                        "mapped_scores": [MagicMock()],
                        "error_message": "Reference metadata missing from mapping results",
                    }
                ),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED
        assert "score_set_id" in result.data

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "Reference metadata missing from mapping results" in sample_score_set.mapping_errors["error_message"]

        # Verify no annotations were created
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 0

    async def test_map_variants_for_score_set_nonexistent_target_gene(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when the target gene does not exist."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(
                    {
                        "mapped_scores": [MagicMock()],
                        "reference_sequences": {"some_key": "some_value"},
                        "target_mappings": [
                            {
                                "target_gene_identifier": "some_key",
                                "alignment_level": "g",
                                "tool_name": "dcd-mapping",
                                "tool_version": "pytest.0.0",
                            }
                        ],
                    }
                ),
            ),
            pytest.raises(ValueError),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify no annotations were created
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 0

    async def test_map_variants_for_score_set_returns_variants_not_in_score_set(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when variants not in score set are returned."""
        # Add a non-existent variant to the mapped output to ensure at least one invalid mapping
        mapping_output = await construct_mock_mapping_output(
            session=session, score_set=sample_score_set, with_layers={"g", "c", "p"}
        )
        mapping_output["mapped_scores"].append({"variant_id": "not_in_score_set", "some_other_data": "value"})

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=self.dummy_mapping_output(mapping_output),
            ),
            pytest.raises(NoResultFound),
        ):
            await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify no annotations were created
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 0

    async def test_map_variants_for_score_set_success_missing_gene_info(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with missing gene info."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=False,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the gene info is missing from the target gene reference sequence
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is None

        # Verify that a mapping record was created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 1

        # Verify that annotation statuses were created and correct
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].annotation_type == "vrs_mapping"
        assert annotation_statuses[0].status == "success"

    @pytest.mark.parametrize(
        "with_layers",
        [
            {"g"},
            {"c"},
            {"p"},
            {"g", "c"},
            {"g", "p"},
            {"c", "p"},
            {"g", "c", "p"},
        ],
    )
    async def test_map_variants_for_score_set_success_layer_permutations(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        with_layers,
    ):
        """Test successful mapping variants with annotation layer permutations."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers=with_layers,
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the annotation layers presence/absence
        for target in sample_score_set.target_genes:
            if "g" in with_layers:
                assert target.pre_mapped_metadata["genomic"] is not None
                assert target.post_mapped_metadata["genomic"] is not None
                pre_mapped_comparator = TEST_GENOMIC_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["genomic"] == pre_mapped_comparator
                assert target.post_mapped_metadata["genomic"] == TEST_GENOMIC_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("genomic") is None

            if "c" in with_layers:
                assert target.pre_mapped_metadata["cdna"] is not None
                assert target.post_mapped_metadata["cdna"] is not None
                pre_mapped_comparator = TEST_CODING_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["cdna"] == pre_mapped_comparator
                assert target.post_mapped_metadata["cdna"] == TEST_CODING_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("cdna") is None

            if "p" in with_layers:
                assert target.pre_mapped_metadata["protein"] is not None
                assert target.post_mapped_metadata["protein"] is not None
                pre_mapped_comparator = TEST_PROTEIN_LAYER["computed_reference_sequence"].copy()
                for key in EXCLUDED_PREMAPPED_ANNOTATION_KEYS:
                    pre_mapped_comparator.pop(key, None)

                assert target.pre_mapped_metadata["protein"] == pre_mapped_comparator
                assert target.post_mapped_metadata["protein"] == TEST_PROTEIN_LAYER["mapped_reference_sequence"]
            else:
                assert target.post_mapped_metadata.get("protein") is None

        # Verify that a mapping record was created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 1

        # Verify that annotation statuses were created and correct
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].annotation_type == "vrs_mapping"
        assert annotation_statuses[0].status == "success"

    async def test_persists_cdna_target_gene_mapping_with_reference_accession_and_null_qc(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """An identity cdna TargetGeneMapping (a cdna layer with no per-variant scores
        joining it) persists with its reference_accession (NM_) and null QC/counts -- the
        artifact reverse translation consumes to resolve the projection transcript."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()

        # Genomic assay layer + an identity cdna layer (no cdna per-variant scores).
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session, score_set=sample_score_set, with_layers={"g", "c"}
            )

        with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=dummy_mapping_job()):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )
        assert result.status == JobStatus.SUCCEEDED

        cdna_tgms = (
            session.query(TargetGeneMapping).filter(TargetGeneMapping.alignment_level == AnnotationLayer.cdna).all()
        )
        assert len(cdna_tgms) == len(sample_score_set.target_genes)
        tgm = cdna_tgms[0]
        assert tgm.reference_accession == "NM_999999.1"
        # Identity row: no mapped_variant joins it, so QC/counts are null.
        assert tgm.total_variants is None
        assert tgm.variants_mapped_cleanly is None
        assert tgm.percent_identity is None

    async def test_map_variants_for_score_set_success_no_successful_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with no successful mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=False,  # Missing post-mapped
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant in the score set to be mapped
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors["error_message"] == "All variants failed to map."

        # Verify that one mapping record was created. Although no successful mapping, an entry is still created.
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 1

        # Verify that the mapping record has no authoritative (post-mapped) allele
        assert authoritative_allele_for(session, mapping_records[0]) is None

        # Verify that annotation statuses were created and correct
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 1
        assert annotation_statuses[0].annotation_type == "vrs_mapping"
        assert annotation_statuses[0].status == "failed"

    async def test_map_variants_for_score_set_incomplete_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with incomplete mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=False,  # Only some variants mapped
            )

        # Create two variants in the score set to be mapped
        variant1 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
            urn="variant:1",
        )
        variant2 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
            urn="variant:2",
        )
        session.add_all([variant1, variant2])
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.incomplete
        assert sample_score_set.mapping_errors is None

        # Although only one variant was successfully mapped, verify that an entity was created
        # for each variant in the score set
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 2

        # Verify that exactly one mapping record has post-mapped (authoritative) allele data
        records_with_post_data = [r for r in mapping_records if authoritative_allele_for(session, r) is not None]
        records_without_post_data = [r for r in mapping_records if authoritative_allele_for(session, r) is None]
        assert len(records_with_post_data) == 1
        assert len(records_without_post_data) == 1

        # Verify that annotation statuses were created and correct
        annotation_status_success = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, VariantAnnotationStatus.status == "success")
            .all()
        )
        assert len(annotation_status_success) == 1
        assert annotation_status_success[0].annotation_type == "vrs_mapping"
        annotation_status_failed = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, VariantAnnotationStatus.status == "failed")
            .all()
        )
        assert len(annotation_status_failed) == 1
        assert annotation_status_failed[0].annotation_type == "vrs_mapping"

    async def test_map_variants_for_score_set_benign_outcomes_are_not_failures(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """A score set whose only unmapped variants are benign absences (intronic / no
        protein consequence) is ``complete``, not ``incomplete``/``failed``: benign
        outcomes carry no allele but are skips, not failures."""

        async def dummy_mapping_job():
            mapping_output = await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )
            # Re-stamp the emitted records as benign absences: no allele, no failure. The
            # helper only produces MAPPED/FAILED, so we override here to exercise the path.
            benign_outcomes = ["intronic", "no_protein_consequence"]
            for idx, mapped_score in enumerate(mapping_output["mapped_scores"]):
                mapped_score["pre_mapped"] = {}
                mapped_score["post_mapped"] = {}
                mapped_score["outcome"] = benign_outcomes[idx % len(benign_outcomes)]
            return mapping_output

        variant1 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
            urn="variant:1",
        )
        variant2 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
            urn="variant:2",
        )
        session.add_all([variant1, variant2])
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        # The run succeeded and the score set is complete -- nothing genuinely failed.
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data["mapped_count"] == 0
        assert result.data["failed_count"] == 0
        assert result.data["skipped_count"] == 2
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # A record exists per variant, but with no authoritative allele.
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 2
        assert all(authoritative_allele_for(session, r) is None for r in mapping_records)

        # Benign outcomes are SKIPPED (not FAILED), and the finer outcome is preserved in metadata.
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 2
        assert all(s.status == "skipped" for s in annotation_statuses)
        assert all(s.failure_category is None for s in annotation_statuses)
        recorded_outcomes = {s.annotation_metadata["outcome"] for s in annotation_statuses}
        assert recorded_outcomes == {"intronic", "no_protein_consequence"}

    async def test_map_variants_for_score_set_complete_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test successful mapping variants with complete mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,  # All variants mapped
            )

        # Create two variants in the score set to be mapped
        variant1 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
            urn="variant:1",
        )
        variant2 = Variant(
            score_set_id=sample_score_set.id,
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
            urn="variant:2",
        )
        session.add_all([variant1, variant2])
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 2

        # Verify that both variants have post-mapped data. I'm comfortable assuming the
        # data is correct given our layer permutation tests above.
        for urn in ["variant:1", "variant:2"]:
            mapping_record = session.query(MappingRecord).filter(MappingRecord.variant.has(urn=urn)).one_or_none()
            assert mapping_record is not None
            assert authoritative_allele_for(session, mapping_record) is not None
            assert mapping_record.hgvs_assay_level is not None

        # Verify that annotation statuses were created and correct
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 2
        for status in annotation_statuses:
            assert status.annotation_type == "vrs_mapping"
            assert status.status == "success"

    async def test_map_variants_for_score_set_updates_existing_mapped_variants(
        self,
        with_independent_processing_runs,
        session,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants updates existing mapped variants."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Create a variant and associated mapped data/annotation status in the score set to be updated
        variant = Variant(
            score_set_id=sample_score_set.id, hgvs_nt="NM_000000.1:c.1A>G", hgvs_pro="NP_000000.1:p.Met1Val", data={}
        )
        session.add(variant)
        session.commit()
        mapping_record = MappingRecord(
            variant_id=variant.id,
            assay_level=AnnotationLayer.genomic,
            mapped_date=date(2023, 1, 1),
            mapping_api_version="v1.0.0",
        )
        session.add(mapping_record)
        session.commit()
        # Link the prior record to an authoritative allele. Re-mapping must retire this link too,
        # not just the record — a live link dangling off a retired record breaks the temporal model.
        prior_allele = Allele(vrs_digest="ga4gh:VA.prior", level=AnnotationLayer.genomic.value)
        session.add(prior_allele)
        session.commit()
        prior_link = MappingRecordAllele(
            mapping_record_id=mapping_record.id, allele_id=prior_allele.id, is_authoritative=True
        )
        session.add(prior_link)
        session.commit()
        variant_annotation_status = VariantAnnotationStatus(
            variant_id=variant.id, current=True, annotation_type="vrs_mapping", status="success"
        )
        session.add(variant_annotation_status)
        session.commit()

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
                JobManager(session, mock_worker_ctx["redis"], sample_independent_variant_mapping_run.id),
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify the existing mapping record was retired (valid_to closed)
        non_current_mapping_record = (
            session.query(MappingRecord)
            .filter(MappingRecord.id == mapping_record.id, MappingRecord.valid_to.isnot(None))
            .one_or_none()
        )
        assert non_current_mapping_record is not None

        # Verify the prior record's authoritative allele link was retired alongside the record,
        # so no live link dangles off the retired record.
        session.refresh(prior_link)
        assert prior_link.valid_to is not None

        # Verify a new, live mapping record entry was created
        new_mapping_record = (
            session.query(MappingRecord)
            .filter(MappingRecord.variant_id == variant.id, MappingRecord.current)
            .one_or_none()
        )
        assert new_mapping_record is not None

        # Gap-free handoff: the prior record closed at exactly the new record's valid_from (one
        # supersession timestamp), and the cascade closed the prior link at that same instant — so
        # no point-in-time query lands in a hole between the old and new record or their links.
        assert non_current_mapping_record.valid_to == new_mapping_record.valid_from
        assert prior_link.valid_to == non_current_mapping_record.valid_to

        # Verify that the new mapping record has updated mapping data
        assert new_mapping_record.mapped_date != date(2023, 1, 1)
        assert new_mapping_record.mapping_api_version != "v1.0.0"

        # Verify the non-current annotation status still exists
        old_annotation_status = (
            session.query(VariantAnnotationStatus)
            .filter(
                VariantAnnotationStatus.variant_id == non_current_mapping_record.variant_id,
                VariantAnnotationStatus.current.is_(False),
            )
            .one_or_none()
        )
        assert old_annotation_status is not None

        # Verify that a new annotation status was created
        new_annotation_status = (
            session.query(VariantAnnotationStatus)
            .filter(VariantAnnotationStatus.variant_id == variant.id, VariantAnnotationStatus.current.is_(True))
            .one_or_none()
        )
        assert new_annotation_status is not None


@pytest.mark.integration
@pytest.mark.asyncio
class TestMapVariantsForScoreSetIntegration:
    """Integration tests for map_variants_for_score_set job."""

    async def test_map_variants_for_score_set_independent_job(
        self,
        session,
        with_independent_processing_runs,
        mock_s3_client,
        mock_worker_ctx,
        sample_independent_variant_creation_run,
        sample_independent_variant_mapping_run,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
    ):
        """Test mapping variants for an independent processing run."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            result = await map_variants_for_score_set(mock_worker_ctx, sample_independent_variant_mapping_run.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that target gene info was updated
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is not None
            assert target.post_mapped_metadata is not None

        # Verify that each variant has a corresponding mapping record
        variants = (
            session.query(Variant)
            .join(MappingRecord, MappingRecord.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappingRecord.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that each variant has an annotation status
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_pipeline_context(
        self,
        session,
        with_variant_creation_pipeline_runs,
        with_variant_mapping_pipeline_runs,
        mock_s3_client,
        mock_worker_ctx,
        sample_pipeline_variant_creation_run,
        sample_pipeline_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
    ):
        """Test mapping variants for a pipeline processing run."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_pipeline_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            result = await map_variants_for_score_set(mock_worker_ctx, sample_pipeline_variant_mapping_run.id)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        # Verify that mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that target gene info was updated
        for target in sample_score_set.target_genes:
            assert target.mapped_hgnc_name is not None
            assert target.post_mapped_metadata is not None

        # Verify that each variant has a corresponding mapping record
        variants = (
            session.query(Variant)
            .join(MappingRecord, MappingRecord.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappingRecord.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that each variant has an annotation status
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status was updated. We expect RUNNING here because
        # the mapping job is not the only job in our dummy pipeline.
        pipeline_run = (
            session.query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.RUNNING

    async def test_map_variants_for_score_set_empty_mapping_results(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no mapping results are returned."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return {}

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=dummy_mapping_job()),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert (
            "Mapping results were not returned from VRS mapping service"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_no_mapped_scores(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no variants are mapped."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=False,
                with_reference_metadata=True,
                with_mapped_scores=False,  # No mapped scores
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # Error message originates from our mock mapping construction function
        assert "test error: no mapped scores" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_no_reference_data(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants when no reference data is provided."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=False,  # No reference metadata
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "Reference metadata missing from mapping results" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_updates_current_mapped_variants(
        self,
        session,
        mock_s3_client,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_independent_variant_creation_run,
    ):
        """Test mapping variants updates current mapped variants even if no changes occur."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            mock_worker_ctx,
            sample_independent_variant_creation_run,
        )

        # Associate mapping records with all variants just created in the score set
        variants = session.query(Variant).filter(Variant.score_set_id == sample_score_set.id).all()
        for variant in variants:
            mapping_record = MappingRecord(
                variant_id=variant.id,
                assay_level=AnnotationLayer.genomic,
                mapped_date=date(2023, 1, 1),
                mapping_api_version="v1.0.0",
            )
            annotation_status = VariantAnnotationStatus(
                variant_id=variant.id, current=True, annotation_type="vrs_mapping", status="success"
            )
            session.add(annotation_status)
            session.add(mapping_record)
        session.commit()

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED

        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that mapping records were marked as non-current and new entries created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == len(variants) * 2  # Each variant has two mapping records now
        for variant in variants:
            non_current_mapping_record = (
                session.query(MappingRecord)
                .filter(MappingRecord.variant_id == variant.id, MappingRecord.valid_to.isnot(None))
                .one_or_none()
            )
            assert non_current_mapping_record is not None

            new_mapping_record = (
                session.query(MappingRecord)
                .filter(MappingRecord.variant_id == variant.id, MappingRecord.current)
                .one_or_none()
            )
            assert new_mapping_record is not None

            # Verify that the new mapping record has updated mapping data
            assert new_mapping_record.mapped_date != date(2023, 1, 1)
            assert new_mapping_record.mapping_api_version != "v1.0.0"

        # Verify that annotation statuses where marked as non-current and new entries created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == len(variants) * 2  # Each variant has two annotation statuses now
        for variant in variants:
            old_annotation_status = (
                session.query(VariantAnnotationStatus)
                .filter(VariantAnnotationStatus.variant_id == variant.id, VariantAnnotationStatus.current.is_(False))
                .one_or_none()
            )
            assert old_annotation_status is not None

            new_annotation_status = (
                session.query(VariantAnnotationStatus)
                .filter(VariantAnnotationStatus.variant_id == variant.id, VariantAnnotationStatus.current.is_(True))
                .one_or_none()
            )
            assert new_annotation_status is not None

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_no_variants(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when no variants exist in the score set."""

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_failure") as mock_send_slack_job_failure,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        mock_send_slack_job_failure.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.FAILED

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        assert "test error: no mapped scores" in sample_score_set.mapping_errors["error_message"]

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.FAILED

    async def test_map_variants_for_score_set_exception_in_mapping(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            result = await map_variants_for_score_set(
                mock_worker_ctx,
                sample_independent_variant_mapping_run.id,
            )

        mock_send_slack_job_error.assert_called_once()
        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.ERRORED
        assert isinstance(result.exception, ValueError)
        # exception messages are persisted in internal properties
        assert "test exception during mapping" in str(result.exception)

        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.ERRORED


@pytest.mark.integration
@pytest.mark.asyncio
class TestMapVariantsForScoreSetArqContext:
    """Integration tests for map_variants_for_score_set job using ARQ worker context."""

    async def test_create_variants_for_score_set_with_arq_context_independent_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_independent_processing_runs,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_independent_variant_creation_run,
        sample_independent_variant_mapping_run,
    ):
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            standalone_worker_context,
            sample_independent_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_independent_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that each variant has a corresponding mapping record
        variants = (
            session.query(Variant)
            .join(MappingRecord, MappingRecord.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappingRecord.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that each variant has an annotation status
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

    async def test_map_variants_for_score_set_with_arq_context_pipeline_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_variant_creation_pipeline_runs,
        with_variant_mapping_pipeline_runs,
        with_populated_domain_data,
        mock_s3_client,
        sample_score_dataframe,
        sample_count_dataframe,
        sample_score_set,
        sample_pipeline_variant_creation_run,
        sample_pipeline_variant_mapping_run,
    ):
        """Test mapping variants for a pipeline processing run using ARQ context."""

        # First, create variants in the score set
        await create_variants_in_score_set(
            session,
            mock_s3_client,
            sample_score_dataframe,
            sample_count_dataframe,
            standalone_worker_context,
            sample_pipeline_variant_creation_run,
        )

        async def dummy_mapping_job():
            return await construct_mock_mapping_output(
                session=session,
                score_set=sample_score_set,
                with_gene_info=True,
                with_layers={"g", "c", "p"},
                with_pre_mapped=True,
                with_post_mapped=True,
                with_reference_metadata=True,
                with_mapped_scores=True,
                with_all_variants=True,
            )

        # Mock mapping output
        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
        ):
            # Now, map variants for the score set
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_pipeline_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        # Verify that mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 4

        # Verify score set mapping state
        assert sample_score_set.mapping_state == MappingState.complete
        assert sample_score_set.mapping_errors is None

        # Verify that each variant has a corresponding mapping record
        variants = (
            session.query(Variant)
            .join(MappingRecord, MappingRecord.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id, MappingRecord.current.is_(True))
            .all()
        )
        assert len(variants) == 4

        # Verify that each variant has an annotation status
        annotation_statuses = (
            session.query(VariantAnnotationStatus)
            .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
            .filter(Variant.score_set_id == sample_score_set.id)
            .all()
        )
        assert len(annotation_statuses) == 4

        # Verify that the job status was updated
        processing_run = (
            session.query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.SUCCEEDED

        # Verify that the pipeline run status was updated. We expect RUNNING here because
        # the mapping job is not the only job in our dummy pipeline.
        pipeline_run = (
            session.query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.RUNNING

    async def test_map_variants_for_score_set_with_arq_context_generic_exception_handling(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_independent_processing_runs,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants with ARQ context when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_independent_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_independent_variant_mapping_run.__class__)
            .filter(sample_independent_variant_mapping_run.__class__.id == sample_independent_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.ERRORED

    async def test_map_variants_for_score_set_with_arq_context_generic_exception_in_pipeline_ctx(
        self,
        session,
        arq_redis,
        arq_worker,
        standalone_worker_context,
        with_variant_mapping_pipeline_runs,
        sample_pipeline_variant_mapping_run,
        sample_score_set,
    ):
        """Test mapping variants with ARQ context in pipeline when an exception occurs during mapping."""

        # Network requests occur within an event loop. Mock result of mapping call
        # with return value from run_in_executor.
        async def dummy_mapping_job():
            raise ValueError("test exception during mapping")

        with (
            patch.object(
                _UnixSelectorEventLoop,
                "run_in_executor",
                return_value=dummy_mapping_job(),
            ),
            patch("mavedb.worker.lib.decorators.job_management.send_slack_job_error") as mock_send_slack_job_error,
        ):
            await arq_redis.enqueue_job("map_variants_for_score_set", sample_pipeline_variant_mapping_run.id)
            await arq_worker.async_run()
            await arq_worker.run_check()

        mock_send_slack_job_error.assert_called_once()
        assert sample_score_set.mapping_state == MappingState.failed
        assert sample_score_set.mapping_errors is not None
        # but replaced with generic error message for external visibility
        assert (
            "Encountered an unexpected error while parsing mapped variants"
            in sample_score_set.mapping_errors["error_message"]
        )

        # Verify that no mapping records were created
        mapping_records = session.query(MappingRecord).all()
        assert len(mapping_records) == 0

        # Verify that no annotation statuses were created
        annotation_statuses = session.query(VariantAnnotationStatus).all()
        assert len(annotation_statuses) == 0

        # Verify that the job status was updated.
        processing_run = (
            session.query(sample_pipeline_variant_mapping_run.__class__)
            .filter(sample_pipeline_variant_mapping_run.__class__.id == sample_pipeline_variant_mapping_run.id)
            .one()
        )
        assert processing_run.status == JobStatus.ERRORED

        # Verify that the pipeline run status was updated to FAILED.
        pipeline_run = (
            session.query(sample_pipeline_variant_mapping_run.pipeline.__class__)
            .filter(
                sample_pipeline_variant_mapping_run.pipeline.__class__.id
                == sample_pipeline_variant_mapping_run.pipeline.id
            )
            .one()
        )
        assert pipeline_run.status == PipelineStatus.FAILED

        # Verify that other jobs in the pipeline were skipped
        for job_run in pipeline_run.job_runs:
            if job_run.id != sample_pipeline_variant_mapping_run.id:
                assert job_run.status == JobStatus.SKIPPED
