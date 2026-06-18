# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

import contextlib
from asyncio.unix_events import _UnixSelectorEventLoop
from datetime import timedelta
from unittest.mock import MagicMock, patch

from variant_annotation.lib.translation.types import TranslationError, TranslationResult, WtCodonMode

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.job_run import JobRun
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.models.variant_annotation_status import VariantAnnotationStatus
from mavedb.worker.jobs.variant_processing.mapping import map_variants_for_score_set
from mavedb.worker.jobs.variant_processing.reverse_translation import (
    _build_translation_config,
    reverse_translate_variants_for_score_set,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from tests.helpers.constants import TEST_GA4GH_IDENTIFIER
from tests.helpers.util.setup.worker import construct_mock_mapping_output

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")

# Module under test, used as the patch root for the external annotation-package calls.
RT_MODULE = "mavedb.worker.jobs.variant_processing.reverse_translation"


class FakeVrsVariation:
    """Stand-in for a ga4gh.vrs variation returned by translate_hgvs_to_variation.

    The reverse translation job only reads ``.id`` (for the VRS digest / dedup key)
    and ``.model_dump()`` (persisted into the ``post_mapped`` JSONB column), so the
    fake exposes exactly those. ``vrs_type`` lets a test assert whether a candidate
    became a single Allele or a CisPhasedBlock.
    """

    def __init__(self, vrs_id: str, vrs_type: str = "Allele"):
        self.id = vrs_id
        self.vrs_type = vrs_type

    def model_dump(self, **kwargs) -> dict:
        # Accept (and ignore) model_dump kwargs like exclude_none, mirroring the real VRS model.
        return {"type": self.vrs_type, "id": self.id}


def fake_construct(results_by_hgvs: dict, errors_by_hgvs: dict | None = None):
    """Build a stand-in for ``construct_equivalent_variants``.

    The job correlates each TranslationResult/TranslationError back to its originating
    mapping record via object identity on ``result.input``, so the fake must echo the
    exact VariantInput instances it was handed. Candidates are keyed by the input's
    assay-level HGVS string.
    """
    errors_by_hgvs = errors_by_hgvs or {}

    def _construct(inputs, *, transcripts, coordinates, config):
        results, errors = [], []
        for inp in inputs:
            if inp.hgvs in errors_by_hgvs:
                errors.append(TranslationError(input=inp, error=errors_by_hgvs[inp.hgvs]))
            elif inp.hgvs in results_by_hgvs:
                entry = results_by_hgvs[inp.hgvs]
                c_candidates, g_candidates = entry[0], entry[1]
                hgvs_p = entry[2] if len(entry) > 2 else None
                results.append(
                    TranslationResult(
                        input=inp,
                        hgvs_c_candidates=list(c_candidates),
                        hgvs_g_candidates=list(g_candidates),
                        hgvs_p=hgvs_p,
                    )
                )
            else:
                errors.append(TranslationError(input=inp, error=f"no stub result for {inp.hgvs!r}"))
        return results, errors

    return _construct


def fake_translate(id_by_hgvs: dict, type_by_hgvs: dict | None = None, errors_by_hgvs: dict | None = None):
    """Build a stand-in for ``translate_hgvs_to_variation`` mapping candidate HGVS -> VRS id.

    Unknown HGVS get a deterministic id derived from the string so distinct candidates
    stay distinct; supplying explicit ids lets a test force two candidates to collapse.
    ``type_by_hgvs`` lets a test mark a bracketed candidate as a ``CisPhasedBlock`` (the
    real translate_hgvs_to_variation returns one for cis-phased multivariants).
    ``errors_by_hgvs`` makes a candidate raise, standing in for an HGVS form ga4gh cannot
    translate to VRS.
    """
    type_by_hgvs = type_by_hgvs or {}
    errors_by_hgvs = errors_by_hgvs or {}

    def _translate(hgvs: str, translator=None) -> FakeVrsVariation:
        if hgvs in errors_by_hgvs:
            raise ValueError(errors_by_hgvs[hgvs])
        return FakeVrsVariation(
            id_by_hgvs.get(hgvs, f"ga4gh:VA.{hgvs}"),
            type_by_hgvs.get(hgvs, "Allele"),
        )

    return _translate


async def _map_variants(session, mock_worker_ctx, mapping_run, score_set, with_layers=frozenset({"g", "c", "p"})):
    """Run the (mocked) mapping job so the score set's variants gain authoritative
    MappingRecords/Alleles — the real inputs the reverse translation job reads."""

    async def dummy_mapping_job():
        return await construct_mock_mapping_output(session=session, score_set=score_set, with_layers=with_layers)

    with patch.object(_UnixSelectorEventLoop, "run_in_executor", return_value=dummy_mapping_job()):
        result = await map_variants_for_score_set(
            mock_worker_ctx,
            mapping_run.id,
            JobManager(session, mock_worker_ctx["redis"], mapping_run.id),
        )

    assert result.status == JobStatus.SUCCEEDED
    session.commit()


async def _reverse_translate(session, mock_worker_ctx, rt_run):
    # The job opens a UTA-backed TranscriptSource to back codon_at (WtCodonMode.ALL).
    # construct_equivalent_variants is mocked in these tests, so the source is never
    # queried -- stub the factory with a no-op context yielding a dummy client.
    with patch(f"{RT_MODULE}.uta_transcript_source", lambda: contextlib.nullcontext(MagicMock())):
        return await reverse_translate_variants_for_score_set(
            mock_worker_ctx,
            rt_run.id,
            JobManager(session, mock_worker_ctx["redis"], rt_run.id),
        )


def _cross_level_statuses(session, score_set_id, status=None):
    query = (
        session.query(VariantAnnotationStatus)
        .join(Variant, VariantAnnotationStatus.variant_id == Variant.id)
        .filter(
            Variant.score_set_id == score_set_id,
            VariantAnnotationStatus.annotation_type == "cross_level_translation",
        )
    )
    if status is not None:
        query = query.filter(VariantAnnotationStatus.status == status)
    return query.all()


def _non_authoritative_links(session):
    return session.query(MappingRecordAllele).filter(MappingRecordAllele.is_authoritative.is_(False)).all()


@pytest.mark.unit
class TestBuildTranslationConfig:
    """Unit tests for _build_translation_config — the optional translation_config job param."""

    def test_none_uses_job_defaults(self):
        config = _build_translation_config(None)
        assert config.include_indels is True
        assert config.wt_codon_mode == WtCodonMode.ALL

    def test_empty_dict_uses_job_defaults(self):
        config = _build_translation_config({})
        assert config.include_indels is True
        assert config.wt_codon_mode == WtCodonMode.ALL

    def test_overrides_win_and_string_wt_codon_mode_is_coerced(self):
        config = _build_translation_config({"wt_codon_mode": "unambiguous", "max_indel_size": 7})
        assert config.wt_codon_mode == WtCodonMode.UNAMBIGUOUS
        assert config.max_indel_size == 7
        assert config.include_indels is True  # untouched default preserved

    def test_can_disable_indels_with_none_mode(self):
        config = _build_translation_config({"wt_codon_mode": "none", "include_indels": False})
        assert config.wt_codon_mode == WtCodonMode.NONE
        assert config.include_indels is False

    def test_rejects_invalid_wt_codon_mode_with_actionable_message(self):
        with pytest.raises(ValueError, match=r"wt_codon_mode 'bogus'.*Valid values"):
            _build_translation_config({"wt_codon_mode": "bogus"})

    def test_rejects_wt_codon_mode_without_indels(self):
        # TranslationConfig enforces: a wt_codon_mode other than "none" requires include_indels.
        with pytest.raises(ValueError, match="translation_config"):
            _build_translation_config({"wt_codon_mode": "all", "include_indels": False})

    def test_rejects_unknown_field_and_lists_valid_options(self):
        with pytest.raises(
            ValueError, match=r"Unknown translation_config option\(s\): not_a_real_field.*Valid options"
        ):
            _build_translation_config({"not_a_real_field": 1})


@pytest.mark.unit
@pytest.mark.asyncio
class TestReverseTranslateVariantsForScoreSetUnit:
    """Unit tests for the reverse_translate_variants_for_score_set job."""

    async def test_no_mapping_records_is_a_noop(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """With no current, post-mapped mapping records there is nothing to translate."""
        result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert isinstance(result, JobExecutionOutcome)
        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 0, "failed": 0, "skipped": 0, "alleles_created": 0}

        # No candidate alleles, links, or annotations were produced.
        assert session.query(Allele).count() == 0
        assert _non_authoritative_links(session) == []
        assert _cross_level_statuses(session, sample_score_set.id) == []

    async def test_creates_genomic_and_coding_candidate_alleles(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A mapped variant expands into one non-authoritative allele per equivalence-class
        candidate, each linked to the originating mapping record."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct({assay_hgvs: ([c_candidate], [g_candidate])})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2}

        # Two non-authoritative links were created, both for our one mapping record.
        non_auth_links = _non_authoritative_links(session)
        assert len(non_auth_links) == 2

        genomic_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.genomic").one()
        assert genomic_allele.level == AnnotationLayer.genomic.value
        assert genomic_allele.hgvs_g == g_candidate
        assert genomic_allele.hgvs_c is None
        assert genomic_allele.transcript == "NC_000001.11"
        assert genomic_allele.post_mapped == {"type": "Allele", "id": "ga4gh:VA.genomic"}

        coding_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.coding").one()
        assert coding_allele.level == AnnotationLayer.cdna.value
        assert coding_allele.hgvs_c == c_candidate
        assert coding_allele.hgvs_g is None
        assert coding_allele.transcript == "NM_000001.1"

        # The candidate alleles are linked to the same mapping record as the authoritative allele.
        mapping_record = session.query(MappingRecord).filter(MappingRecord.variant_id == variant.id).one()
        assert {link.mapping_record_id for link in non_auth_links} == {mapping_record.id}

        statuses = _cross_level_statuses(session, sample_score_set.id)
        assert len(statuses) == 1
        assert statuses[0].status == "success"

    async def test_transcript_is_queryable_via_derived_expression(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Allele.transcript is a derived hybrid_property — filtering on it in SQL resolves to
        the accession of the populated HGVS column, with no stored transcript column."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct({assay_hgvs: ([c_candidate], [g_candidate])})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        # The SQL expression derives the accession from the populated HGVS column.
        genomic = session.query(Allele).filter(Allele.transcript == "NC_000001.11").all()
        assert [a.vrs_digest for a in genomic] == ["ga4gh:VA.genomic"]
        coding = session.query(Allele).filter(Allele.transcript == "NM_000001.1").all()
        assert [a.vrs_digest for a in coding] == ["ga4gh:VA.coding"]

    async def test_protein_consequence_is_persisted_as_a_protein_allele(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """The deterministic protein consequence (``result.hgvs_p``) is emitted as the
        protein-level member of the equivalence set -- a non-authoritative ``level=protein``
        allele linked to the record, not dropped."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        # c_to_p emits a predicted consequence in parens; the job strips them before use.
        p_consequence = "NP_000001.1:p.(Met1Val)"
        p_stripped = "NP_000001.1:p.Met1Val"
        construct = fake_construct({assay_hgvs: ([c_candidate], [g_candidate], p_consequence)})
        translate = fake_translate(
            {
                g_candidate: "ga4gh:VA.genomic",
                c_candidate: "ga4gh:VA.coding",
                p_stripped: "ga4gh:VA.protein",  # the stripped form is what reaches the translator
            }
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        protein_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.protein").one()
        assert protein_allele.level == AnnotationLayer.protein.value
        assert protein_allele.hgvs_p == p_stripped  # stored without the prediction parens
        # Linked to the record as a non-authoritative member of the equivalence set.
        assert protein_allele.id in {link.allele_id for link in _non_authoritative_links(session)}

    async def test_cis_phased_multivariant_candidate_is_persisted_as_a_block(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A bracketed genomic candidate (non-adjacent codon components) is translated into a
        CisPhasedBlock and persisted like any other candidate allele, keyed by its CPB digest."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        block_candidate = "NC_000001.11:g.[1000A>G;1002T>C]"
        construct = fake_construct({assay_hgvs: ([], [block_candidate])})
        translate = fake_translate(
            {block_candidate: "ga4gh:CPB.block"},
            type_by_hgvs={block_candidate: "CisPhasedBlock"},
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1}

        block_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:CPB.block").one()
        assert block_allele.level == AnnotationLayer.genomic.value
        # The full bracketed expression is stored verbatim; its accession anchors the row.
        assert block_allele.hgvs_g == block_candidate
        assert block_allele.transcript == "NC_000001.11"
        assert block_allele.post_mapped == {"type": "CisPhasedBlock", "id": "ga4gh:CPB.block"}

        assert len(_non_authoritative_links(session)) == 1

    async def test_duplicate_candidate_digests_are_deduped_per_record(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """When a coding and genomic candidate resolve to the same VRS digest, only one
        allele/link is written for that mapping record (per the library's dedup contract)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct({assay_hgvs: ([c_candidate], [g_candidate])})
        # Both candidates collapse to the same VRS digest.
        translate = fake_translate({g_candidate: "ga4gh:VA.shared", c_candidate: "ga4gh:VA.shared"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1}
        assert len(_non_authoritative_links(session)) == 1
        assert session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.shared").count() == 1

    async def test_candidate_equal_to_authoritative_allele_is_not_relinked(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A derived candidate whose digest equals the record's authoritative (assay-measured)
        allele is not linked again — the record already links that allele authoritatively, and a
        derived duplicate would surface the measured variant twice. No new link and no new allele;
        an empty derived set is still a success with nothing to add."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        # Exactly one allele exists after mapping: the authoritative post-mapped allele.
        assert session.query(Allele).count() == 1
        authoritative_allele = session.query(Allele).one()
        assert authoritative_allele.vrs_digest == TEST_GA4GH_IDENTIFIER

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct({assay_hgvs: ([c_candidate], [])})
        # The candidate resolves to the same digest as the authoritative allele.
        translate = fake_translate({c_candidate: TEST_GA4GH_IDENTIFIER})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 0}

        # The authoritative allele was reused, not duplicated.
        assert session.query(Allele).count() == 1
        # No derived link was added: the candidate equals the record's authoritative allele.
        assert _non_authoritative_links(session) == []

    async def test_independent_rerun_retires_prior_links_and_keeps_current_stable(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """An independent second run is idempotent: it retires the prior derived links (closing
        valid_to) and writes fresh live ones, so the set of *current* non-authoritative links is
        stable while the superseded links are retained as history — the partial unique index on
        live links is never violated and no alleles are duplicated."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct({assay_hgvs: ([c_candidate], [g_candidate])})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        # First run: two live derived links.
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            first = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert first.status == JobStatus.SUCCEEDED
        first_links = _non_authoritative_links(session)
        assert len(first_links) == 2
        assert all(link.valid_to is None for link in first_links)

        # Second, independent run with identical inputs.
        rerun = JobRun(
            urn="test:reverse_translate_variants_for_score_set:rerun",
            job_type="reverse_translate_variants_for_score_set",
            job_function="reverse_translate_variants_for_score_set",
            max_retries=3,
            retry_count=0,
            job_params=dict(sample_independent_reverse_translation_run.job_params),
        )
        session.add(rerun)
        session.commit()

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            second = await _reverse_translate(session, mock_worker_ctx, rerun)

        assert second.status == JobStatus.SUCCEEDED

        links = _non_authoritative_links(session)
        live = [link for link in links if link.valid_to is None]
        retired = [link for link in links if link.valid_to is not None]

        # The live set is stable (still exactly the two candidates) and the prior run's links are
        # retained as closed history rather than deleted.
        assert len(live) == 2
        assert len(retired) == 2

        # Gap-free handoff: the prior links closed at exactly the instant the new links opened, under
        # one supersession timestamp — so a point-in-time query never lands in a hole between runs,
        # even though the translation loop ran (and could have committed) between retire and insert.
        assert {link.valid_to for link in retired} == {link.valid_from for link in live}
        assert len({link.valid_from for link in live}) == 1

        # The two candidate alleles are reused across runs, not duplicated.
        assert session.query(Allele).filter(Allele.vrs_digest.in_(["ga4gh:VA.genomic", "ga4gh:VA.coding"])).count() == 2

        # The cross-level translation status is versioned the same way: prior retired, one current.
        statuses = _cross_level_statuses(session, sample_score_set.id)
        assert len(statuses) == 2
        assert len([s for s in statuses if s.current]) == 1

    async def test_all_failures_fail_the_job(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """If every variant's translation errors, the job fails and records FAILED annotations."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        construct = fake_construct({}, errors_by_hgvs={assay_hgvs: "forward translation failed"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.FAILED
        assert result.data == {"translated": 0, "failed": 1, "skipped": 0, "alleles_created": 0}

        assert _non_authoritative_links(session) == []
        failed_statuses = _cross_level_statuses(session, sample_score_set.id, status="failed")
        assert len(failed_statuses) == 1
        assert failed_statuses[0].error_message == "forward translation failed"
        # Library/input-level failures still carry the assay-level HGVS as metadata.
        assert failed_statuses[0].annotation_metadata == {"hgvs_input": assay_hgvs}

    async def test_partial_success_succeeds_with_mixed_annotations(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """One variant translating and another erroring yields a SUCCEEDED job with both a
        success and a failure annotation."""
        variant_ok = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        variant_err = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:2",
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
        )
        session.add_all([variant_ok, variant_err])
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        g_candidate = "NC_000001.11:g.1000A>G"
        construct = fake_construct(
            {"NM_000000.1:c.1A>G": ([], [g_candidate])},
            errors_by_hgvs={"NM_000000.1:c.2G>T": "no consequence"},
        )
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 1, "skipped": 0, "alleles_created": 1}

        assert len(_cross_level_statuses(session, sample_score_set.id, status="success")) == 1
        assert len(_cross_level_statuses(session, sample_score_set.id, status="failed")) == 1
        assert len(_non_authoritative_links(session)) == 1

    async def test_partial_candidate_translation_failure_keeps_success_with_metadata(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """One candidate translating and another failing VRS translation leaves the variant a
        SUCCESS (one allele created) while retaining the dropped candidate in metadata."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        good_candidate = "NC_000001.11:g.1000A>G"
        bad_candidate = "NC_000001.11:g.999A>T"
        construct = fake_construct({assay_hgvs: ([], [good_candidate, bad_candidate])})
        translate = fake_translate(
            {good_candidate: "ga4gh:VA.genomic"},
            errors_by_hgvs={bad_candidate: "untranslatable form"},
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1}
        assert len(_non_authoritative_links(session)) == 1

        statuses = _cross_level_statuses(session, sample_score_set.id, status="success")
        assert len(statuses) == 1
        failed_candidates = statuses[0].annotation_metadata["failed_candidates"]
        assert failed_candidates == [{"hgvs": bad_candidate, "level": "genomic", "error": "untranslatable form"}]

    async def test_all_candidates_failing_translation_marks_variant_failed(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """When every candidate fails VRS translation, the variant is FAILED, no allele is
        created, and the per-candidate errors are retained in metadata."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        bad_candidate = "NC_000001.11:g.999A>T"
        construct = fake_construct({assay_hgvs: ([], [bad_candidate])})
        translate = fake_translate({}, errors_by_hgvs={bad_candidate: "untranslatable form"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.FAILED
        assert result.data == {"translated": 0, "failed": 1, "skipped": 0, "alleles_created": 0}
        assert _non_authoritative_links(session) == []

        failed_statuses = _cross_level_statuses(session, sample_score_set.id, status="failed")
        assert len(failed_statuses) == 1
        assert failed_statuses[0].error_message == "All candidate HGVS failed VRS translation."
        metadata = failed_statuses[0].annotation_metadata
        assert metadata["hgvs_input"] == assay_hgvs
        assert metadata["failed_candidates"] == [
            {"hgvs": bad_candidate, "level": "genomic", "error": "untranslatable form"}
        ]

    async def test_no_coding_transcript_is_skipped_not_failed(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A target gene aligned only at the genomic level has no coding transcript, so its
        variants are SKIPPED (no protein consequence) rather than attempted and failed."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Genomic-only mapping: no cdna TargetGeneMapping, so no coding transcript to anchor on.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0}

        # No translation attempted: no candidate alleles or links, and the variant is
        # recorded as SKIPPED rather than FAILED.
        assert _non_authoritative_links(session) == []
        assert _cross_level_statuses(session, sample_score_set.id, status="failed") == []
        assert len(_cross_level_statuses(session, sample_score_set.id, status="skipped")) == 1

    async def test_genomic_accession_coding_target_is_reverse_translated(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A genomic-accession (NC_:g.) coding variant -- previously skipped for want of a
        coding transcript -- is reverse-translated once the mapper emits a cdna
        TargetGeneMapping, whose reference_accession anchors the projection."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Genomic assay layer + cdna identity TargetGeneMapping (carrying the NM_); the
        # genomic variant projects onto that transcript for reverse translation.
        await _map_variants(
            session,
            mock_worker_ctx,
            sample_independent_variant_mapping_run,
            sample_score_set,
            with_layers={"g", "c"},
        )

        assay_hgvs = "NC_000001.11:g.1000A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({assay_hgvs: ([], [g_candidate])})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({g_candidate: "ga4gh:VA.genomic"})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1}
        assert _cross_level_statuses(session, sample_score_set.id, status="skipped") == []

    def _run_mapped_date(self, session, target_gene_id):
        """The mapped_date the (mock) mapping run stamped on its TargetGeneMappings."""
        return (
            session.query(TargetGeneMapping)
            .filter(
                TargetGeneMapping.target_gene_id == target_gene_id,
                TargetGeneMapping.alignment_level == AnnotationLayer.genomic,
            )
            .first()
            .mapped_date
        )

    async def test_uses_latest_cdna_row_within_the_run(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """When a run has more than one cdna TargetGeneMapping for a target (same run date),
        RT reverse-translates against the newest (highest id), not an arbitrary one."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Mapping emits a cdna TargetGeneMapping (NM_999999.1) stamped with the run date.
        await _map_variants(
            session,
            mock_worker_ctx,
            sample_independent_variant_mapping_run,
            sample_score_set,
            with_layers={"g", "c"},
        )
        target_gene = sample_score_set.target_genes[0]
        run_date = self._run_mapped_date(session, target_gene.id)
        # A newer cdna row (higher id) for the same target and same run date.
        session.add(
            TargetGeneMapping(
                target_gene_id=target_gene.id,
                alignment_level=AnnotationLayer.cdna,
                reference_accession="NM_111111.1",
                preferred=False,
                tool_version="pytest.0.0",
                mapped_date=run_date,
            )
        )
        session.commit()

        assay_hgvs = "NC_000001.11:g.1000A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: ([], [g_candidate])})
        captured: dict = {}

        def capturing_construct(inputs, *, transcripts, coordinates, config):
            captured["transcripts"] = [inp.transcript for inp in inputs]
            return delegate(inputs, transcripts=transcripts, coordinates=coordinates, config=config)

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", capturing_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({g_candidate: "ga4gh:VA.g"})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        # The newest cdna row within the run wins, not the first-mapped one.
        assert captured["transcripts"] == ["NM_111111.1"]

    async def test_ignores_stale_cdna_row_from_a_different_run(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A cdna row left behind by a *different* run (different run date) is not used:
        the current run emitted no cdna row, so the variant is skipped (transcript
        unresolved) rather than reverse-translated against the stale transcript -- the
        mapped_date anchor narrows the accumulation edge case (mavedb-api#763)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Current run maps genomic only -- no cdna row for this run's date.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )
        target_gene = sample_score_set.target_genes[0]
        run_date = self._run_mapped_date(session, target_gene.id)
        # A leftover cdna row from a *prior* run (an earlier date) must not be picked up.
        session.add(
            TargetGeneMapping(
                target_gene_id=target_gene.id,
                alignment_level=AnnotationLayer.cdna,
                reference_accession="NM_888888.1",
                preferred=False,
                tool_version="pytest.0.0",
                mapped_date=run_date - timedelta(days=1),
            )
        )
        session.commit()

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0}
        skipped = _cross_level_statuses(session, sample_score_set.id, status="skipped")
        assert len(skipped) == 1
        assert skipped[0].annotation_metadata["skip_category"] == "transcript_unresolved"

    async def test_coding_target_skip_is_classified_recoverable(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A protein-coding target with no resolvable coding transcript is a *recoverable*
        skip -- classified ``transcript_unresolved`` so it is findable, distinct from a
        regulatory target's correct skip."""
        assert sample_score_set.target_genes[0].category == TargetCategory.protein_coding
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Genomic-only mapping: no cdna TargetGeneMapping, so no coding transcript resolves.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0}
        skipped = _cross_level_statuses(session, sample_score_set.id, status="skipped")
        assert len(skipped) == 1
        assert skipped[0].annotation_metadata["skip_category"] == "transcript_unresolved"

    async def test_regulatory_target_skip_is_classified_correct(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A non-coding/regulatory target has no protein consequence: its skip is *correct*,
        classified ``no_coding_transcript`` rather than the recoverable category."""
        target = sample_score_set.target_genes[0]
        target.category = TargetCategory.regulatory
        session.add(target)
        session.commit()

        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0}
        skipped = _cross_level_statuses(session, sample_score_set.id, status="skipped")
        assert len(skipped) == 1
        assert skipped[0].annotation_metadata["skip_category"] == "no_coding_transcript"

    async def test_translation_config_param_overrides_job_defaults(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A translation_config job param overrides the job's TranslationConfig defaults, and the
        resulting config is passed through to construct_equivalent_variants."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: ([], [g_candidate])})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic"})

        captured: dict = {}

        def capturing_construct(inputs, *, transcripts, coordinates, config):
            captured["config"] = config
            return delegate(inputs, transcripts=transcripts, coordinates=coordinates, config=config)

        run = sample_independent_reverse_translation_run
        run.job_params = {**run.job_params, "translation_config": {"wt_codon_mode": "unambiguous", "max_indel_size": 7}}
        session.add(run)
        session.commit()

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", capturing_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, run)

        assert result.status == JobStatus.SUCCEEDED
        config = captured["config"]
        assert config.wt_codon_mode == WtCodonMode.UNAMBIGUOUS
        assert config.max_indel_size == 7
        # Defaults the param did not override are preserved.
        assert config.include_indels is True

    async def test_translation_config_defaults_when_param_absent(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """With no translation_config param the job falls back to its sensible defaults
        (full codon equivalence class, indels included)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: ([], [g_candidate])})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic"})

        captured: dict = {}

        def capturing_construct(inputs, *, transcripts, coordinates, config):
            captured["config"] = config
            return delegate(inputs, transcripts=transcripts, coordinates=coordinates, config=config)

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", capturing_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        config = captured["config"]
        assert config.include_indels is True
        assert config.wt_codon_mode == WtCodonMode.ALL

    async def test_protein_level_transcript_resolved_via_uta(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A protein-only mapping has no cdna alignment transcript, so its coding transcript
        is resolved NP_→NM_ via UTA and supplied as the VariantInput hint."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn="variant:1",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        # Protein-only mapping: no cdna TargetGeneMapping, so the transcript must come from UTA.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"p"}
        )

        assay_hgvs = "NP_000000.1:p.Met1Val"
        g_candidate = "NC_000001.11:g.1000A>G"
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic"})

        # Capture the transcript hint each VariantInput was built with, then delegate to the
        # normal stub for the translation result.
        captured_hints: dict[str, str | None] = {}
        delegate = fake_construct({assay_hgvs: ([], [g_candidate])})

        def capturing_construct(inputs, *, transcripts, coordinates, config):
            captured_hints.update({inp.hgvs: inp.transcript for inp in inputs})
            return delegate(inputs, transcripts=transcripts, coordinates=coordinates, config=config)

        with (
            patch(f"{RT_MODULE}._coding_transcripts_for_proteins", lambda accessions: {"NP_000000.1": "NM_000111.1"}),
            patch(f"{RT_MODULE}.construct_equivalent_variants", capturing_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1}
        # The NP_ accession was resolved to its NM_ transcript and passed as the hint.
        assert captured_hints == {assay_hgvs: "NM_000111.1"}
