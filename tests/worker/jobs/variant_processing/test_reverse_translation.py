# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

import contextlib
from asyncio.unix_events import _UnixSelectorEventLoop
from datetime import timedelta
from unittest.mock import MagicMock, patch

from variant_annotation.lib.translation.types import (
    ProjectionPair,
    TranslationError,
    TranslationErrorReason,
    TranslationResult,
    WtCodonMode,
)

from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.vrs_utils import canonical_variation_document
from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.job_run import JobRun
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.worker.jobs.variant_processing.mapping import map_variants_for_score_set
from mavedb.worker.jobs.variant_processing.reverse_translation import (
    _build_translation_config,
    cdna_transcript_resolver,
    reverse_translate_variants_for_score_set,
)
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.projection import project_measured_variants
from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE
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


def fake_construct(
    results_by_hgvs: dict,
    errors_by_hgvs: dict | None = None,
    not_translatable_by_hgvs: dict | None = None,
    upstream_unavailable_by_hgvs: dict | None = None,
):
    """Build a stand-in for ``construct_equivalent_variants``.

    The job correlates each TranslationResult/TranslationError back to its originating
    mapping record via object identity on ``result.input``, so the fake must echo the
    exact VariantInput instances it was handed. Results are keyed by the input's
    assay-level HGVS string.

    Each result spec is one of:

    * a list of ``(hgvs_c, hgvs_g)`` pairs — one ProjectionPair each.
      ``hgvs_g`` may be ``None`` for a projection-failed, coding-only projection pair.
    * a ``(pairs, hgvs_p)`` tuple pairing that list with a shared protein apex.

    Each pair becomes one ProjectionPair, mirroring the position-aligned
    coding↔genomic rows the real reverse-translate CLI emits (with ``--one-row-per-input``).

    ``errors_by_hgvs`` maps an assay HGVS to a genuine-failure message (reason ``FAILED``);
    ``not_translatable_by_hgvs`` maps one to a benign non-translatable message (reason
    ``NOT_TRANSLATABLE``), standing in for an edit type the library screens out up front;
    ``upstream_unavailable_by_hgvs`` maps one to a UTA outage message (reason ``UPSTREAM_UNAVAILABLE``).
    """
    errors_by_hgvs = errors_by_hgvs or {}
    not_translatable_by_hgvs = not_translatable_by_hgvs or {}
    upstream_unavailable_by_hgvs = upstream_unavailable_by_hgvs or {}

    def _construct(inputs, *, transcripts, coordinates, config):
        results, errors = [], []
        for inp in inputs:
            if inp.hgvs in not_translatable_by_hgvs:
                errors.append(
                    TranslationError(
                        input=inp,
                        error=not_translatable_by_hgvs[inp.hgvs],
                        reason=TranslationErrorReason.NOT_TRANSLATABLE,
                    )
                )
            elif inp.hgvs in upstream_unavailable_by_hgvs:
                errors.append(
                    TranslationError(
                        input=inp,
                        error=upstream_unavailable_by_hgvs[inp.hgvs],
                        reason=TranslationErrorReason.UPSTREAM_UNAVAILABLE,
                    )
                )
            elif inp.hgvs in errors_by_hgvs:
                errors.append(TranslationError(input=inp, error=errors_by_hgvs[inp.hgvs]))
            elif inp.hgvs in results_by_hgvs:
                spec = results_by_hgvs[inp.hgvs]
                # A bare list is the candidate pairs; a tuple pairs them with a protein apex.
                pairs, hgvs_p = (spec[0], spec[1]) if isinstance(spec, tuple) else (spec, None)
                results.append(
                    TranslationResult(
                        input=inp,
                        projection_pairs=[ProjectionPair(hgvs_c=hgvs_c, hgvs_g=hgvs_g) for hgvs_c, hgvs_g in pairs],
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


class FakeCoordinates:
    """Deterministic stand-in for ``WorkerCoordinateTranslator``, used by the measured projection step.

    The worker context's ``hdp`` is a bare mock, so the real translator would produce projection errors
    rather than strings. Tables pin a projection per input HGVS; anything else projects to a fixed
    placeholder so a test that doesn't care still gets a well-formed measured group.
    """

    def __init__(
        self,
        c_by_g: dict | None = None,
        g_by_c: dict | None = None,
        p_by_c: dict | None = None,
        mane: str | None = None,
    ):
        self.c_by_g = c_by_g or {}
        self.g_by_c = g_by_c or {}
        self.p_by_c = p_by_c or {}
        self.mane = mane
        self.mane_lookups: list[str] = []

    def g_to_c(self, g_hgvs: str, transcript: str) -> str:
        return self.c_by_g.get(g_hgvs, f"{transcript}:c.1A>G")

    def normalize(self, hgvs_string: str) -> str:
        return hgvs_string

    def c_to_g(self, c_hgvs: str) -> str:
        return self.g_by_c.get(c_hgvs, "NC_000001.11:g.1000A>G")

    def c_to_p(self, c_hgvs: str) -> str:
        return self.p_by_c.get(c_hgvs, "NP_999999.1:p.(Met1Val)")

    # The tables hold changes whose spans match on both levels, so the unnormalized projections agree.
    def c_to_g_literal(self, c_hgvs: str) -> str:
        return self.c_to_g(c_hgvs)

    def g_to_c_literal(self, g_hgvs: str, transcript: str) -> str:
        return self.g_to_c(g_hgvs, transcript)

    def c_to_n(self, c_hgvs: str) -> str:
        return c_hgvs.replace(":c.", ":n.")

    def mane_select_transcript(self, g_hgvs: str) -> str | None:
        self.mane_lookups.append(g_hgvs)
        return self.mane


# Mapping recomputes an incoming allele's identity from its own content rather than trusting the
# producer's `id`, so a stored authoritative allele carries this rather than the fixture's placeholder.
# The fold-in tests below depend on the reverse-translated coding allele identifying to the same value.
CANONICAL_AUTHORITATIVE_DIGEST = canonical_variation_document(
    TEST_VALID_POST_MAPPED_VRS_ALLELE, subject="mapping fixture"
)[1]


# Candidates for the rerun tests: the measured coding allele with its genomic partner, and an unrelated pair.
MEASURED_C = "NM_000001.1:c.5A>G"
MEASURED_PARTNER_G = "NC_000001.11:g.1000A>G"
OTHER_C = "NM_000001.1:c.9C>T"
OTHER_G = "NC_000001.11:g.1004C>T"


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


async def _reverse_translate(session, mock_worker_ctx, rt_run, coordinates=None):
    # The job opens a UTA-backed TranscriptSource to back codon_at (WtCodonMode.ALL).
    # construct_equivalent_variants is mocked in these tests, so the source is never
    # queried -- stub the factory with a no-op context yielding a dummy client. The measured
    # projection step reads the coordinate translator, so it gets a deterministic fake.
    coordinates = coordinates or FakeCoordinates()
    with (
        patch(f"{RT_MODULE}.uta_transcript_source", lambda: contextlib.nullcontext(MagicMock())),
        patch(f"{RT_MODULE}.WorkerCoordinateTranslator", lambda hdp: coordinates),
    ):
        return await reverse_translate_variants_for_score_set(
            mock_worker_ctx,
            rt_run.id,
            JobManager(session, mock_worker_ctx["redis"], rt_run.id),
        )


def _cross_level_events(session, score_set_id, disposition=None, reason=None):
    query = (
        session.query(AnnotationEvent)
        .join(Variant, AnnotationEvent.variant_id == Variant.id)
        .filter(
            Variant.score_set_id == score_set_id,
            AnnotationEvent.annotation_type == "cross_level_translation",
        )
    )
    if disposition is not None:
        query = query.filter(AnnotationEvent.disposition == disposition)
    if reason is not None:
        query = query.filter(AnnotationEvent.reason == reason)
    return query.all()


def _non_authoritative_links(session):
    return session.query(MappingRecordAllele).filter(MappingRecordAllele.is_authoritative.is_(False)).all()


def _live_links(session, record_id):
    """Every live (valid_to IS NULL) link for a mapping record, authoritative or derived."""
    return (
        session.query(MappingRecordAllele)
        .filter(
            MappingRecordAllele.mapping_record_id == record_id,
            MappingRecordAllele.valid_to.is_(None),
        )
        .all()
    )


def _authoritative_link(session, record_id):
    return (
        session.query(MappingRecordAllele)
        .filter(
            MappingRecordAllele.mapping_record_id == record_id,
            MappingRecordAllele.is_authoritative.is_(True),
            MappingRecordAllele.valid_to.is_(None),
        )
        .one()
    )


def _record_for(session, variant_id):
    return session.query(MappingRecord).filter(MappingRecord.variant_id == variant_id).one()


def _group(session, group_id):
    return session.get(ProjectionGroup, group_id)


def _measured_group(session, record_id):
    """The projection group the record's live authoritative link points at."""
    return _group(session, _authoritative_link(session, record_id).projection_group)


def _live_groups(session, record_id):
    return (
        session.query(ProjectionGroup)
        .filter(ProjectionGroup.mapping_record_id == record_id, ProjectionGroup.valid_to.is_(None))
        .all()
    )


def _authoritative_versions(session, record_id):
    """Every version of the record's authoritative link, oldest first."""
    return (
        session.query(MappingRecordAllele)
        .filter(
            MappingRecordAllele.mapping_record_id == record_id,
            MappingRecordAllele.is_authoritative.is_(True),
        )
        .order_by(MappingRecordAllele.id)
        .all()
    )


@pytest.mark.unit
class TestBuildTranslationConfig:
    """Unit tests for _build_translation_config — the optional translation_config job param."""

    def test_none_uses_job_defaults(self):
        config = _build_translation_config(None)
        assert config.include_indels is True
        assert config.wt_codon_mode == WtCodonMode.ALL

    def test_defaults_ride_out_a_minutes_long_uta_outage(self):
        config = _build_translation_config(None)
        assert config.upstream_max_attempts == 6
        assert config.upstream_retry_backoff_seconds == 5.0

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
        assert result.data == {"translated": 0, "failed": 0, "skipped": 0, "alleles_created": 0, "fold_in_missed": 0}

        # No candidate alleles, links, or annotations were produced.
        assert session.query(Allele).count() == 0
        assert _non_authoritative_links(session) == []
        assert _cross_level_events(session, sample_score_set.id) == []

    async def test_same_day_remap_without_a_cdna_row_skips_rather_than_binding_a_stale_transcript(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
        map_variants_sample_params,
    ):
        """#763: a record binds its transcript to its own run via job_run_id. When a later same-day remap
        emits no cdna row, reverse translation must resolve no transcript and skip -- not fall back to an
        earlier same-day run's cdna transcript, which the day-granular key alone could not tell apart."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()

        # Run 1 (all layers) writes a cdna TargetGeneMapping; its transcript must not leak to run 2.
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        # Run 2 -- same score set, same calendar day -- emits genomic + protein only, no cdna row. Its
        # records supersede run 1's and are the current ones reverse translation reads.
        second_mapping_run = JobRun(
            urn="test:map_variants_for_score_set:2",
            job_type="map_variants_for_score_set",
            job_function="map_variants_for_score_set",
            max_retries=3,
            retry_count=0,
            job_params=map_variants_sample_params,
        )
        session.add(second_mapping_run)
        session.commit()
        await _map_variants(session, mock_worker_ctx, second_mapping_run, sample_score_set, with_layers={"g", "p"})

        # construct_equivalent_variants is never reached for a skipped variant.
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", fake_construct({})),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["translated"] == 0
        assert result.data["skipped"] == 1
        assert result.data["alleles_created"] == 0
        assert _non_authoritative_links(session) == []
        assert len(_cross_level_events(session, sample_score_set.id, reason="transcript_unresolved")) == 1

    async def test_cis_phased_assay_hgvs_is_skipped_before_the_engine(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A cis-phased multivariant assay HGVS (``g.[a;b]``) cannot be forward-translated -- the
        engine's parser rejects the allele list at the opening ``[``. The job screens it out up front
        and records a benign NOT_APPLICABLE / cis_phased_unsupported skip, never handing it to the
        engine (which would otherwise count it as a failure the drop gate reads as a regression)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        # Map first so the variant gains a transcript-bearing authoritative record: the skip must fire
        # on the cis-phased shape even though a transcript resolves, not as a transcript-unresolved skip.
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        cis_phased = "NC_000016.10:g.[1314031A>T;1320511T>C]"
        record = _record_for(session, variant.id)
        record.hgvs_assay_level = cis_phased
        session.commit()

        def _fail_if_engine_sees_it(inputs, *, transcripts, coordinates, config):
            assert list(inputs) == [], "a cis-phased input must be screened out before the engine"
            return [], []

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", _fail_if_engine_sees_it),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}
        assert _non_authoritative_links(session) == []

        events = _cross_level_events(session, sample_score_set.id, reason="cis_phased_unsupported")
        assert len(events) == 1
        assert events[0].disposition == "not_applicable"
        assert events[0].event_metadata["hgvs_input"] == cis_phased

    async def test_falls_back_to_mapped_date_when_the_record_has_no_job_run_id(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A record with no job_run_id (pre-column, or reshaped from legacy) resolves its cdna transcript
        via the day-granular (target_gene_id, mapped_date) fallback rather than skipping."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        # Simulate the pre-job_run_id / reshaped state: no TargetGeneMapping carries a run anchor, so
        # resolution has only the day-granular key to work with.
        session.query(TargetGeneMapping).filter(
            TargetGeneMapping.target_gene_id == sample_score_set.target_genes[0].id
        ).update({TargetGeneMapping.job_run_id: None}, synchronize_session=False)
        session.commit()

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        construct = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        # The transcript resolved via the fallback, so the variant translated instead of skipping.
        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2, "fold_in_missed": 1}

    @pytest.mark.parametrize("record_has_job_run_id", [False, True])
    async def test_legacy_record_falls_back_to_the_target_post_mapped_cdna_accession(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
        record_has_job_run_id,
    ):
        """Legacy mappings wrote no cdna TargetGeneMapping; their coding accession lives in the target's
        post_mapped_metadata. A record with no job_run_id falls back to it, but a record anchored to a run
        resolves only through that run's own cdna row, so the legacy accession never leaks into it."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Genomic only: no cdna TargetGeneMapping for either key to find.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"cdna": {"sequence_accessions": ["NM_777777.1", "NM_777778.1"]}}
        if not record_has_job_run_id:
            session.query(TargetGeneMapping).filter(TargetGeneMapping.target_gene_id == target_gene.id).update(
                {TargetGeneMapping.job_run_id: None}, synchronize_session=False
            )
        session.commit()

        record = _record_for(session, variant.id)
        tgm = session.get(TargetGeneMapping, record.target_gene_mapping_id)
        resolve = cdna_transcript_resolver(session, sample_score_set.id, FakeCoordinates())
        expected = None if record_has_job_run_id else "NM_777777.1"
        assert resolve(tgm.job_run_id, target_gene.id, tgm.mapped_date, "NC_000001.11:g.1000A>G") == expected

        assay_hgvs = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: [("NM_777777.1:c.5A>G", None)]})
        captured: dict = {}

        def capturing_construct(inputs, *, transcripts, coordinates, config):
            captured["transcripts"] = [inp.transcript for inp in inputs]
            return delegate(inputs, transcripts=transcripts, coordinates=coordinates, config=config)

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", capturing_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        if record_has_job_run_id:
            assert captured["transcripts"] == []
            assert result.data["skipped"] == 1
            assert len(_cross_level_events(session, sample_score_set.id, reason="transcript_unresolved")) == 1
        else:
            assert captured["transcripts"] == ["NM_777777.1"]
            assert result.data["translated"] == 1

    @pytest.mark.parametrize("record_has_job_run_id", [True, False])
    async def test_genomic_only_target_falls_back_to_its_transcript_accession_then_mane_select(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        record_has_job_run_id,
    ):
        """A genomic-only target has no cdna row and no legacy cdna accession. A transcript target
        accession is used as is; otherwise a protein-coding target takes the MANE Select transcript at the
        record's locus, looked up once per target gene."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )
        target_gene = sample_score_set.target_genes[0]
        target_gene.category = TargetCategory.protein_coding
        target_gene.post_mapped_metadata = {"genomic": {"sequence_accessions": ["NC_000001.11"]}}
        if not record_has_job_run_id:
            session.query(TargetGeneMapping).filter(TargetGeneMapping.target_gene_id == target_gene.id).update(
                {TargetGeneMapping.job_run_id: None}, synchronize_session=False
            )
        session.commit()
        record = _record_for(session, variant.id)
        tgm = session.get(TargetGeneMapping, record.target_gene_mapping_id)
        key = (tgm.job_run_id, target_gene.id, tgm.mapped_date)

        coordinates = FakeCoordinates(mane="NM_888888.2")
        resolve = cdna_transcript_resolver(session, sample_score_set.id, coordinates)
        assert resolve(*key, "NC_000001.11:g.1000A>G") == "NM_888888.2"
        assert resolve(*key, "NC_000001.11:g.1001A>G") == "NM_888888.2"
        assert coordinates.mane_lookups == ["NC_000001.11:g.1000A>G"]

        # Protein records pass no genomic HGVS and keep their NP_→NM_ lookup.
        assert resolve(*key, None) is None

        target_gene.target_accession = TargetAccession(accession="NM_999999.3", assembly="GRCh38", gene="GENE")
        session.commit()
        coordinates = FakeCoordinates(mane="NM_888888.2")
        resolve = cdna_transcript_resolver(session, sample_score_set.id, coordinates)
        assert resolve(*key, "NC_000001.11:g.1000A>G") == "NM_999999.3"
        assert coordinates.mane_lookups == []

    async def test_mane_select_is_only_looked_up_for_protein_coding_targets_and_a_bounded_number_of_times(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
    ):
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )
        target_gene = sample_score_set.target_genes[0]
        target_gene.post_mapped_metadata = {"genomic": {"sequence_accessions": ["NC_000001.11"]}}
        target_gene.category = TargetCategory.regulatory
        session.commit()
        record = _record_for(session, variant.id)
        tgm = session.get(TargetGeneMapping, record.target_gene_mapping_id)
        key = (tgm.job_run_id, target_gene.id, tgm.mapped_date)

        coordinates = FakeCoordinates(mane="NM_888888.2")
        resolve = cdna_transcript_resolver(session, sample_score_set.id, coordinates)
        assert resolve(*key, "NC_000001.11:g.1000A>G") is None
        assert coordinates.mane_lookups == []

        target_gene.category = TargetCategory.protein_coding
        session.commit()
        coordinates = FakeCoordinates(mane=None)
        resolve = cdna_transcript_resolver(session, sample_score_set.id, coordinates)
        for position in range(1000, 1010):
            assert resolve(*key, f"NC_000001.11:g.{position}A>G") is None
        assert len(coordinates.mane_lookups) == 5

    @pytest.mark.parametrize("failure", ["projection", "transcript"])
    async def test_a_failed_projection_keeps_the_group_an_earlier_run_wrote(
        self,
        session,
        with_independent_processing_runs,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_score_set,
        failure,
    ):
        """A data provider outage must not erase coding and protein projections: the record keeps its group."""
        assay_hgvs = "NC_000001.11:g.1000A>G"
        variant = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#11", hgvs_nt=assay_hgvs, data={}
        )
        session.add(variant)
        session.commit()
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g"}
        )
        record = _record_for(session, variant.id)

        def resolve(*_):
            return "NM_000001.1"

        project_measured_variants(
            session,
            sample_score_set.id,
            coordinates=FakeCoordinates(c_by_g={assay_hgvs: MEASURED_C}),
            resolve_transcript=resolve,
        )
        session.commit()
        written = _measured_group(session, record.id)
        assert written.hgvs_c == MEASURED_C

        class Unreachable(FakeCoordinates):
            def g_to_c(self, g_hgvs, transcript):
                raise ConnectionError("cdot-rest unreachable")

        def unreachable_resolve(*_):
            raise ConnectionError("cdot-rest unreachable")

        counts = project_measured_variants(
            session,
            sample_score_set.id,
            coordinates=Unreachable() if failure == "projection" else FakeCoordinates(),
            resolve_transcript=resolve if failure == "projection" else unreachable_resolve,
        )
        session.commit()

        assert counts["kept_on_error"] == 1
        assert _measured_group(session, record.id).id == written.id
        assert [group.id for group in _live_groups(session, record.id)] == [written.id]

    async def test_untranslatable_delins_still_gets_a_measured_projection_group(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A multi-codon delins has no codon-level equivalence class, so the library returns NOT_TRANSLATABLE
        and RT links nothing. The measured group comes from the projection step, not from RT, so the record
        still describes its variant at the coding and protein levels."""
        assay_hgvs = "NC_000001.11:g.1000_1005delinsTTT"
        variant = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#11", hgvs_nt=assay_hgvs, data={}
        )
        session.add(variant)
        session.commit()
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g", "c"}
        )

        hgvs_c = "NM_999999.1:c.4_9delinsTTT"
        coordinates = FakeCoordinates(
            c_by_g={assay_hgvs: hgvs_c}, p_by_c={hgvs_c: "NP_999999.1:p.(Lys2_Ala3delinsPhe)"}
        )
        construct = fake_construct({}, not_translatable_by_hgvs={assay_hgvs: "multi-codon delins"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(
                session, mock_worker_ctx, sample_independent_reverse_translation_run, coordinates=coordinates
            )

        assert result.status == JobStatus.SUCCEEDED
        assert result.data["skipped"] == 1
        assert _non_authoritative_links(session) == []

        record = _record_for(session, variant.id)
        measured = _measured_group(session, record.id)
        assert measured.current
        assert (measured.hgvs_c, measured.hgvs_g, measured.hgvs_p) == (
            hgvs_c,
            assay_hgvs,
            "NP_999999.1:p.Lys2_Ala3delinsPhe",
        )
        assert [group.id for group in _live_groups(session, record.id)] == [measured.id]

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
        candidate, each linked to the originating mapping record. The two members of a single
        projection pair (coding + genomic projection) share one projection_group."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
        construct = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2, "fold_in_missed": 1}

        # Two non-authoritative links were created, both for our one mapping record.
        non_auth_links = _non_authoritative_links(session)
        assert len(non_auth_links) == 2

        genomic_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.genomic").one()
        assert genomic_allele.level == SequenceLevel.genomic.value
        assert genomic_allele.hgvs_g == g_candidate
        assert genomic_allele.hgvs_c is None
        assert genomic_allele.transcript == "NC_000001.11"
        assert genomic_allele.post_mapped == {"type": "Allele", "id": "ga4gh:VA.genomic"}

        coding_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.coding").one()
        assert coding_allele.level == SequenceLevel.cdna.value
        assert coding_allele.hgvs_c == c_candidate
        assert coding_allele.hgvs_g is None
        assert coding_allele.transcript == "NM_000001.1"

        # The candidate alleles are linked to the same mapping record as the authoritative allele.
        mapping_record = _record_for(session, variant.id)
        assert {link.mapping_record_id for link in non_auth_links} == {mapping_record.id}

        # Both members of the single projection pair share one candidate group, distinct from the measured
        # group, whose strings are the pair's own.
        (group_id,) = {link.projection_group for link in non_auth_links}
        assert group_id != _authoritative_link(session, mapping_record.id).projection_group
        group = _group(session, group_id)
        assert group.current
        assert (group.hgvs_c, group.hgvs_g, group.hgvs_p) == (c_candidate, g_candidate, None)

        events = _cross_level_events(session, sample_score_set.id)
        assert len(events) == 1
        assert events[0].disposition == "present"
        # The event metadata carries the paired projection pair shape, not the removed flat lists.
        assert events[0].event_metadata["candidates"] == [
            {"hgvs_c": c_candidate, "hgvs_g": g_candidate, "variant_type": None, "measured": False}
        ]

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
            urn=f"{sample_score_set.urn}#11",
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
        construct = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
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
        allele linked to the record, not dropped. It is the measured variant's own consequence, so its
        link joins the record's measured group; each candidate group records it as its hgvs_p."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
        construct = fake_construct({assay_hgvs: ([(c_candidate, g_candidate)], p_consequence)})
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
        assert protein_allele.level == SequenceLevel.protein.value
        assert protein_allele.hgvs_p == p_stripped  # stored without the prediction parens

        # Linked to the record as a non-authoritative member of the measured group.
        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group
        protein_link = next(link for link in _non_authoritative_links(session) if link.allele_id == protein_allele.id)
        assert protein_link.projection_group == measured_group_id
        # The coding/genomic pair shares one candidate group, which carries the stripped consequence.
        pair_links = [link for link in _non_authoritative_links(session) if link.allele_id != protein_allele.id]
        (pair_group_id,) = {link.projection_group for link in pair_links}
        assert pair_group_id != measured_group_id
        pair_group = _group(session, pair_group_id)
        assert (pair_group.hgvs_c, pair_group.hgvs_g, pair_group.hgvs_p) == (c_candidate, g_candidate, p_stripped)

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
        """A bracketed genomic projection (non-adjacent codon components) is translated into a
        CisPhasedBlock and persisted like any other candidate allele, keyed by its CPB digest, and
        grouped with its coding partner in the projection pair."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        block_candidate = "NC_000001.11:g.[1000A>G;1002T>C]"
        construct = fake_construct({assay_hgvs: [(c_candidate, block_candidate)]})
        translate = fake_translate(
            {c_candidate: "ga4gh:VA.coding", block_candidate: "ga4gh:CPB.block"},
            type_by_hgvs={block_candidate: "CisPhasedBlock"},
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2, "fold_in_missed": 1}

        block_allele = session.query(Allele).filter(Allele.vrs_digest == "ga4gh:CPB.block").one()
        assert block_allele.level == SequenceLevel.genomic.value
        # The full bracketed expression is stored verbatim; its accession anchors the row.
        assert block_allele.hgvs_g == block_candidate
        assert block_allele.transcript == "NC_000001.11"
        assert block_allele.post_mapped == {"type": "CisPhasedBlock", "id": "ga4gh:CPB.block"}

        # The block (genomic projection) and its coding partner are one projection pair -> one group.
        (group_id,) = {link.projection_group for link in _non_authoritative_links(session)}
        group = _group(session, group_id)
        assert (group.hgvs_c, group.hgvs_g) == (c_candidate, block_candidate)

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
        """When the coding and genomic members of a projection pair resolve to the same VRS digest,
        only one allele/link is written for that mapping record (per the library's dedup contract).
        The surviving link still carries the pair's group."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
        construct = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        # Both members of the projection pair collapse to the same VRS digest.
        translate = fake_translate({g_candidate: "ga4gh:VA.shared", c_candidate: "ga4gh:VA.shared"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1, "fold_in_missed": 1}
        non_auth_links = _non_authoritative_links(session)
        assert len(non_auth_links) == 1
        group = _group(session, non_auth_links[0].projection_group)
        assert (group.hgvs_c, group.hgvs_g) == (c_candidate, g_candidate)
        assert session.query(Allele).filter(Allele.vrs_digest == "ga4gh:VA.shared").count() == 1

    async def test_authoritative_allele_is_folded_into_its_projection_group(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Nucleotide assay: the measured allele appears among the RT candidates (the library makes
        no exclusions). That pair is the measured variant itself, so rather than forming a candidate
        group its other member joins the record's measured group, which then resolves to {canonical
        coding, canonical genomic}. The authoritative link already points at that group and is not
        re-versioned by RT."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        # Exactly one allele exists after mapping: the authoritative (measured) allele.
        assert session.query(Allele).count() == 1
        assert session.query(Allele).one().vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_sibling = "NC_000001.11:g.1000A>G"
        construct = fake_construct({assay_hgvs: [(c_candidate, g_sibling)]})
        # The coding member IS the measured allele (folds in); the genomic member is its projection.
        translate = fake_translate({c_candidate: CANONICAL_AUTHORITATIVE_DIGEST, g_sibling: "ga4gh:VA.genomic"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        # Only the genomic sibling is a newly created derived allele; the coding member folds into
        # the existing authoritative link rather than creating a second link/allele.
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1, "fold_in_missed": 0}

        record = _record_for(session, variant.id)
        auth_link = _authoritative_link(session, record.id)
        derived_links = _non_authoritative_links(session)
        assert len(derived_links) == 1

        # The sibling derived link joins the measured group -- the pairing is resolvable from the
        # authoritative allele's group -- and no candidate group was created for the measured pair.
        measured_group_id = auth_link.projection_group
        assert measured_group_id is not None
        assert derived_links[0].projection_group == measured_group_id
        assert [group.id for group in _live_groups(session, record.id)] == [measured_group_id]

        # The measured group resolves to {canonical c (authoritative), canonical g (projection)}: two live
        # links, one authoritative and one derived.
        group_links = [link for link in _live_links(session, record.id) if link.projection_group == measured_group_id]
        assert len(group_links) == 2
        assert {link.is_authoritative for link in group_links} == {True, False}
        assert auth_link.allele.vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST
        assert derived_links[0].allele.vrs_digest == "ga4gh:VA.genomic"

        # Only the mapping job's link and the projection step's copy exist: RT did not regroup it.
        assert [link.projection_group for link in _authoritative_versions(session, record.id)] == [
            None,
            measured_group_id,
        ]

        metadata = _cross_level_events(session, sample_score_set.id)[0].event_metadata
        assert metadata["fold_in_missed"] is False
        assert metadata["candidates"] == [
            {"hgvs_c": c_candidate, "hgvs_g": g_sibling, "variant_type": None, "measured": True}
        ]

    async def test_authoritative_fold_in_when_measured_at_genomic_level(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """The fold-in is symmetric in the measured level: when the genomic member of a projection pair
        equals the record's authoritative allele, the coding sibling joins the measured group and no
        candidate group is created for the pair."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Genomic assay layer + cdna identity mapping so RT resolves a coding transcript.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"g", "c"}
        )
        assert session.query(Allele).one().vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST

        assay_hgvs = "NC_000001.11:g.1000A>G"
        c_sibling = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        construct = fake_construct({assay_hgvs: [(c_sibling, g_candidate)]})
        # The genomic member IS the measured allele (folds in); the coding member is its projection.
        translate = fake_translate({c_sibling: "ga4gh:VA.coding", g_candidate: CANONICAL_AUTHORITATIVE_DIGEST})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1, "fold_in_missed": 0}

        record = _record_for(session, variant.id)
        auth_link = _authoritative_link(session, record.id)
        derived_links = _non_authoritative_links(session)
        measured_group_id = auth_link.projection_group
        assert measured_group_id is not None
        assert [link.projection_group for link in derived_links] == [measured_group_id]
        assert [group.id for group in _live_groups(session, record.id)] == [measured_group_id]
        assert auth_link.allele.vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST
        assert derived_links[0].allele.vrs_digest == "ga4gh:VA.coding"

    async def test_measured_allele_matching_no_candidate_is_flagged_as_a_missed_fold_in(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Nucleotide assay whose measured allele has a different digest from every RT candidate: the
        measured group holds only the authoritative link, both candidates form a candidate group, and the run
        counts the miss and flags it on the variant's event."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_sibling = "NC_000001.11:g.1000A>G"
        construct = fake_construct({assay_hgvs: [(c_candidate, g_sibling)]})
        # The coding member is the measured change, but it identifies differently from the stored allele.
        translate = fake_translate({c_candidate: "ga4gh:VA.divergent", g_sibling: "ga4gh:VA.genomic"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2, "fold_in_missed": 1}
        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group
        assert [
            link.is_authoritative
            for link in _live_links(session, record.id)
            if link.projection_group == measured_group_id
        ] == [True]
        (candidate_group_id,) = {link.projection_group for link in _non_authoritative_links(session)}
        assert candidate_group_id != measured_group_id
        assert _cross_level_events(session, sample_score_set.id)[0].event_metadata["fold_in_missed"] is True

    async def test_projection_failed_candidate_is_a_one_member_group(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A projection pair whose genomic projection failed (hgvs_g is None) yields a well-formed
        one-member (coding-only) group -- no crash, no desync, and the group holds just the coding
        link. This is the case the old two-list shape silently dropped."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_paired = "NM_000001.1:c.5A>G"
        g_paired = "NC_000001.11:g.1000A>G"
        c_orphan = "NM_000001.1:c.9A>T"  # projection failed: no genomic partner
        construct = fake_construct({assay_hgvs: [(c_paired, g_paired), (c_orphan, None)]})
        translate = fake_translate(
            {
                c_paired: "ga4gh:VA.coding0",
                g_paired: "ga4gh:VA.genomic0",
                c_orphan: "ga4gh:VA.coding1",
            }
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 3, "fold_in_missed": 1}

        non_auth_links = _non_authoritative_links(session)
        groups: dict[tuple[str, str | None], set[str]] = {}
        for link in non_auth_links:
            group = _group(session, link.projection_group)
            groups.setdefault((group.hgvs_c, group.hgvs_g), set()).add(link.allele.vrs_digest)

        # One group is the full projection pair; the other is the projection-failed coding-only member.
        assert groups == {
            (c_paired, g_paired): {"ga4gh:VA.coding0", "ga4gh:VA.genomic0"},
            (c_orphan, None): {"ga4gh:VA.coding1"},
        }

    async def test_protein_assay_measured_group_holds_only_the_protein_over_a_fan_out(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Protein assay: the authoritative allele is the protein itself, which appears in no
        coding/genomic projection pair, so nothing folds in and its measured group describes only
        the protein. The ambiguous fan-out becomes N distinct candidate groups, each a coding/genomic pair."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        # Protein-only mapping: the authoritative allele is the protein allele.
        await _map_variants(
            session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set, with_layers={"p"}
        )
        assert session.query(Allele).one().vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST

        assay_hgvs = "NP_000000.1:p.Met1Val"
        # A degenerate protein consequence -> three coding/genomic projection pairs (the fan-out).
        pairs = [
            ("NM_000001.1:c.3A>G", "NC_000001.11:g.100A>G"),
            ("NM_000001.1:c.3A>C", "NC_000001.11:g.100A>C"),
            ("NM_000001.1:c.3A>T", "NC_000001.11:g.100A>T"),
        ]
        # Protein assay: no protein apex is re-emitted (result.hgvs_p is None) -- the protein
        # allele is already authoritative.
        construct = fake_construct({assay_hgvs: pairs})
        translate = fake_translate({hgvs: f"ga4gh:VA.{hgvs}" for pair in pairs for hgvs in pair})

        with (
            patch(f"{RT_MODULE}._coding_transcripts_for_proteins", lambda accessions: {"NP_000000.1": "NM_000001.1"}),
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 6, "fold_in_missed": 0}

        record = _record_for(session, variant.id)
        # The protein authoritative allele is not part of any pair: its measured group holds only hgvs_p and
        # no derived link joins it.
        auth_link = _authoritative_link(session, record.id)
        assert auth_link.allele.vrs_digest == CANONICAL_AUTHORITATIVE_DIGEST
        measured = _group(session, auth_link.projection_group)
        assert (measured.hgvs_c, measured.hgvs_g, measured.hgvs_p) == (None, None, assay_hgvs)

        # Three distinct candidate groups, each a coding/genomic pair (two members), with no protein
        # consequence re-emitted.
        groups: dict[tuple[str, str, str | None], int] = {}
        for link in _non_authoritative_links(session):
            assert link.projection_group != measured.id
            group = _group(session, link.projection_group)
            key = (group.hgvs_c, group.hgvs_g, group.hgvs_p)
            groups[key] = groups.get(key, 0) + 1
        assert groups == {(hgvs_c, hgvs_g, None): 2 for hgvs_c, hgvs_g in pairs}

    async def test_independent_rerun_supersedes_prior_links_and_candidate_groups(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """An independent second run is idempotent: it retires the prior derived links and candidate
        groups (closing valid_to) and writes fresh live ones, so the set of *current* non-authoritative
        links is stable while the superseded rows are retained as history -- the partial unique index on
        live links is never violated and no alleles are duplicated. The measured group, whose strings are
        unchanged, is left alone."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
        construct = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        translate = fake_translate({g_candidate: "ga4gh:VA.genomic", c_candidate: "ga4gh:VA.coding"})

        # First run: two live derived links sharing one candidate group.
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            first = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert first.status == JobStatus.SUCCEEDED
        first_links = _non_authoritative_links(session)
        assert len(first_links) == 2
        assert all(link.valid_to is None for link in first_links)
        (first_group_id,) = {link.projection_group for link in first_links}
        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group

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

        # The live set is stable (still exactly the two candidates, sharing a fresh candidate group) and
        # the prior run's links and group are retained as closed history rather than deleted.
        assert len(live) == 2
        assert len(retired) == 2
        (second_group_id,) = {link.projection_group for link in live}
        assert second_group_id != first_group_id
        first_group, second_group = _group(session, first_group_id), _group(session, second_group_id)
        assert first_group.valid_to == second_group.valid_from
        assert (second_group.hgvs_c, second_group.hgvs_g) == (first_group.hgvs_c, first_group.hgvs_g)

        # The measured group and the authoritative link pointing at it are untouched by the rerun.
        assert _authoritative_link(session, record.id).projection_group == measured_group_id
        assert _group(session, measured_group_id).current
        assert len(_authoritative_versions(session, record.id)) == 2

        # Gap-free handoff: the prior links closed at exactly the instant the new links opened, under
        # one supersession timestamp -- so a point-in-time query never lands in a hole between runs,
        # even though the translation loop ran (and could have committed) between retire and insert.
        assert {link.valid_to for link in retired} == {link.valid_from for link in live}
        assert len({link.valid_from for link in live}) == 1

        # The two candidate alleles are reused across runs, not duplicated.
        assert session.query(Allele).filter(Allele.vrs_digest.in_(["ga4gh:VA.genomic", "ga4gh:VA.coding"])).count() == 2

        # The cross-level translation log is append-only: the rerun adds a second event, and
        # "current" is the latest by id (there is no current flag to desync).
        events = _cross_level_events(session, sample_score_set.id)
        assert len(events) == 2
        assert max(events, key=lambda e: e.id).disposition == "present"

    @pytest.mark.parametrize(
        "rerun_pairs, rerun_unavailable, expected_live, candidate_group_kept",
        [
            # The measured pair moves position: its partner still joins the measured group.
            (
                [(OTHER_C, OTHER_G), (MEASURED_C, MEASURED_PARTNER_G)],
                False,
                {
                    ("ga4gh:VA.partner", "measured"),
                    ("ga4gh:VA.other_c", "candidate"),
                    ("ga4gh:VA.other_g", "candidate"),
                },
                False,
            ),
            # The measured allele no longer appears among the candidates: its partner leaves the measured group.
            (
                [(OTHER_C, OTHER_G)],
                False,
                {("ga4gh:VA.other_c", "candidate"), ("ga4gh:VA.other_g", "candidate")},
                False,
            ),
            # Identical candidates: the candidate group is still re-minted (TODO#765), the measured group is not.
            (
                [(MEASURED_C, MEASURED_PARTNER_G), (OTHER_C, OTHER_G)],
                False,
                {
                    ("ga4gh:VA.partner", "measured"),
                    ("ga4gh:VA.other_c", "candidate"),
                    ("ga4gh:VA.other_g", "candidate"),
                },
                False,
            ),
            # UTA unavailable: the record's prior links and candidate group are still this run's answer.
            (
                [],
                True,
                {
                    ("ga4gh:VA.partner", "measured"),
                    ("ga4gh:VA.other_c", "candidate"),
                    ("ga4gh:VA.other_g", "candidate"),
                },
                True,
            ),
        ],
    )
    async def test_rerun_never_regroups_the_measured_allele(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
        rerun_pairs,
        rerun_unavailable,
        expected_live,
        candidate_group_kept,
    ):
        """The measured group belongs to the projection step, so whatever a rerun's candidates look like, the
        authoritative link keeps pointing at the same measured group and is never re-versioned by RT. Only the
        derived links and candidate groups are superseded, under the run's timestamp, unless the record's input
        could not be asked about (UTA unavailable)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        translate = fake_translate(
            {
                MEASURED_C: CANONICAL_AUTHORITATIVE_DIGEST,
                MEASURED_PARTNER_G: "ga4gh:VA.partner",
                OTHER_C: "ga4gh:VA.other_c",
                OTHER_G: "ga4gh:VA.other_g",
            }
        )
        first_construct = fake_construct({assay_hgvs: [(MEASURED_C, MEASURED_PARTNER_G), (OTHER_C, OTHER_G)]})
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", first_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            first = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)
        assert first.status == JobStatus.SUCCEEDED

        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group
        (first_candidate_group_id,) = {
            link.projection_group
            for link in _non_authoritative_links(session)
            if link.projection_group != measured_group_id
        }
        first_live_ids = {link.id for link in _non_authoritative_links(session)}

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

        second_construct = (
            fake_construct({}, upstream_unavailable_by_hgvs={assay_hgvs: "could not connect to server"})
            if rerun_unavailable
            else fake_construct({assay_hgvs: rerun_pairs})
        )
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", second_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            await _reverse_translate(session, mock_worker_ctx, rerun)

        # The measured group and its authoritative link are exactly as the first run's projection left them.
        assert _authoritative_link(session, record.id).projection_group == measured_group_id
        assert _group(session, measured_group_id).current
        assert len(_authoritative_versions(session, record.id)) == 2

        live_derived = [link for link in _non_authoritative_links(session) if link.valid_to is None]
        assert {
            (link.allele.vrs_digest, "measured" if link.projection_group == measured_group_id else "candidate")
            for link in live_derived
        } == expected_live

        first_candidate_group = _group(session, first_candidate_group_id)
        assert first_candidate_group.current is candidate_group_kept
        if candidate_group_kept:
            assert {link.id for link in live_derived} == first_live_ids
        else:
            # Every live candidate link points at a group minted by this run, under the run's timestamp.
            live_candidate_groups = {
                link.projection_group for link in live_derived if link.projection_group != measured_group_id
            }
            assert first_candidate_group_id not in live_candidate_groups
            assert {_group(session, group_id).valid_from for group_id in live_candidate_groups} == {
                first_candidate_group.valid_to
            }

    async def test_a_coding_change_spelled_two_ways_stays_paired_with_its_genomic_projection(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """An insertion at an exon junction can be written on either side of it. Both spellings are one coding
        allele, but only the one inside an exon projects to the genome; the allele joins that pair's group
        rather than a coding-only group, whichever spelling the library lists first."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        junction_c = "NM_000001.1:c.10_11insGAG"
        exon_c = "NM_000001.1:c.11_12insAGG"
        exon_g = "NC_000001.11:g.2000_2001insCCT"
        construct = fake_construct({"NM_000000.1:c.1A>G": [(junction_c, None), (exon_c, exon_g)]})
        translate = fake_translate({junction_c: "ga4gh:VA.shared_c", exon_c: "ga4gh:VA.shared_c", exon_g: "ga4gh:VA.g"})
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group
        candidate_groups: dict[int, set[str]] = {}
        for link in _non_authoritative_links(session):
            if link.valid_to is None and link.projection_group != measured_group_id:
                candidate_groups.setdefault(link.projection_group, set()).add(link.allele.vrs_digest)

        assert list(candidate_groups.values()) == [{"ga4gh:VA.shared_c", "ga4gh:VA.g"}]
        (group_id,) = candidate_groups
        assert (_group(session, group_id).hgvs_c, _group(session, group_id).hgvs_g) == (exon_c, exon_g)

    async def test_a_changed_standalone_projection_detaches_the_measured_groups_rt_alleles(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Rewriting a measured group outside the reverse translation job retires the alleles RT added to it,
        reports them as ``detached_rt_links``, and leaves candidate groups live."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        translate = fake_translate(
            {
                MEASURED_C: CANONICAL_AUTHORITATIVE_DIGEST,
                MEASURED_PARTNER_G: "ga4gh:VA.partner",
                OTHER_C: "ga4gh:VA.other_c",
                OTHER_G: "ga4gh:VA.other_g",
            }
        )
        construct = fake_construct({"NM_000000.1:c.1A>G": [(MEASURED_C, MEASURED_PARTNER_G), (OTHER_C, OTHER_G)]})
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        record = _record_for(session, variant.id)
        measured_group_id = _authoritative_link(session, record.id).projection_group
        attached = [link for link in _non_authoritative_links(session) if link.projection_group == measured_group_id]
        candidates = [link for link in _non_authoritative_links(session) if link.projection_group != measured_group_id]
        assert attached and candidates

        class Shifted(FakeCoordinates):
            def c_to_g(self, c_hgvs):
                return "NC_000001.11:g.2000A>G"

        counts = project_measured_variants(
            session, sample_score_set.id, coordinates=Shifted(), resolve_transcript=lambda *_: None
        )
        session.commit()

        assert counts["detached_rt_links"] == len(attached)
        for link in attached + candidates:
            session.refresh(link)
        assert all(link.valid_to is not None for link in attached)
        assert all(link.valid_to is None for link in candidates)
        assert not _group(session, measured_group_id).current

    @pytest.mark.parametrize(
        "rerun_outcome, keeps_prior_link",
        [
            ("upstream_unavailable", True),
            ("translation_error", False),
            ("not_translatable", False),
            ("vrs_translation_failure", False),
        ],
    )
    async def test_rerun_keeps_prior_links_only_when_upstream_unavailable(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
        rerun_outcome,
        keeps_prior_link,
    ):
        """A variant that fails a rerun because UTA was unavailable keeps the derived links from its last
        successful run, since its inputs are unchanged. Any other failure is the rerun's answer for that
        variant, so its prior links are retired."""
        variant_ok = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#11", hgvs_nt="NM_000000.1:c.1A>G", data={}
        )
        variant_down = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#12", hgvs_nt="NM_000000.1:c.2G>T", data={}
        )
        session.add_all([variant_ok, variant_down])
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        ok_candidate = "NM_000001.1:c.5A>G"
        down_candidate = "NM_000001.1:c.6G>T"
        ids = {ok_candidate: "ga4gh:VA.ok", down_candidate: "ga4gh:VA.down"}
        first_construct = fake_construct(
            {"NM_000000.1:c.1A>G": [(ok_candidate, None)], "NM_000000.1:c.2G>T": [(down_candidate, None)]}
        )
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", first_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate(ids)),
        ):
            first = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)
        assert first.status == JobStatus.SUCCEEDED
        down_link = next(
            link for link in _non_authoritative_links(session) if link.allele.vrs_digest == "ga4gh:VA.down"
        )

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

        ok_result = {"NM_000000.1:c.1A>G": [(ok_candidate, None)]}
        down_message = {"NM_000000.1:c.2G>T": "reverse translation failed"}
        second_translate = fake_translate(ids)
        if rerun_outcome == "upstream_unavailable":
            second_construct = fake_construct(ok_result, upstream_unavailable_by_hgvs=down_message)
        elif rerun_outcome == "translation_error":
            second_construct = fake_construct(ok_result, errors_by_hgvs=down_message)
        elif rerun_outcome == "not_translatable":
            second_construct = fake_construct(ok_result, not_translatable_by_hgvs=down_message)
        else:
            second_construct = first_construct
            second_translate = fake_translate(ids, errors_by_hgvs={down_candidate: "cannot translate to VRS"})
        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", second_construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", second_translate),
        ):
            second = await _reverse_translate(session, mock_worker_ctx, rerun)
        assert second.status == JobStatus.SUCCEEDED

        session.refresh(down_link)
        assert (down_link.valid_to is None) is keeps_prior_link
        # The link's candidate group shares its fate.
        assert _group(session, down_link.projection_group).current is keeps_prior_link
        live_digests = sorted(
            link.allele.vrs_digest for link in _non_authoritative_links(session) if link.valid_to is None
        )
        assert live_digests == (["ga4gh:VA.down", "ga4gh:VA.ok"] if keeps_prior_link else ["ga4gh:VA.ok"])

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
            urn=f"{sample_score_set.urn}#11",
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
        assert result.data == {"translated": 0, "failed": 1, "skipped": 0, "alleles_created": 0, "fold_in_missed": 0}

        assert _non_authoritative_links(session) == []
        failed_events = _cross_level_events(session, sample_score_set.id, disposition="failed")
        assert len(failed_events) == 1
        assert failed_events[0].event_metadata["error_message"] == "forward translation failed"
        # Library/input-level failures still carry the assay-level HGVS as metadata.
        assert failed_events[0].event_metadata["hgvs_input"] == assay_hgvs

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
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        variant_err = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#12",
            hgvs_nt="NM_000000.1:c.2G>T",
            hgvs_pro="NP_000000.1:p.Val2Leu",
            data={},
        )
        session.add_all([variant_ok, variant_err])
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct(
            {"NM_000000.1:c.1A>G": [(c_candidate, None)]},
            errors_by_hgvs={"NM_000000.1:c.2G>T": "no consequence"},
        )
        translate = fake_translate({c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 1, "skipped": 0, "alleles_created": 1, "fold_in_missed": 1}

        assert len(_cross_level_events(session, sample_score_set.id, disposition="present")) == 1
        assert len(_cross_level_events(session, sample_score_set.id, disposition="failed")) == 1
        assert len(_non_authoritative_links(session)) == 1

    async def test_non_translatable_edit_is_skipped_not_failed(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A protein consequence whose edit type has no DNA equivalence class (the library screens it
        out as NOT_TRANSLATABLE) is a benign structural gap, not a failure: it is counted as skipped
        and recorded as (not_applicable, not_translatable) — never in the failed tally. This is the
        #767 fix: such variants must not pollute FAILED for the retroactive backfill."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        construct = fake_construct(
            {},
            not_translatable_by_hgvs={
                assay_hgvs: "Protein edit type has no DNA equivalence class to reverse-translate"
            },
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        # A whole score set of non-translatable variants is a benign no-op, not a job failure.
        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}

        # No alleles/links, and no failed event at all.
        assert _non_authoritative_links(session) == []
        assert _cross_level_events(session, sample_score_set.id, disposition="failed") == []

        # Recorded as a structural gap: not_applicable / not_translatable, carrying the input HGVS.
        events = _cross_level_events(session, sample_score_set.id, reason="not_translatable")
        assert len(events) == 1
        assert events[0].disposition == "not_applicable"
        assert events[0].event_metadata["hgvs_input"] == assay_hgvs

    async def test_non_translatable_skip_coexists_with_success_and_failure(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """Across three variants — one translatable, one non-translatable, one genuinely erroring —
        each lands in its own bucket: translated / skipped / failed. The genuine error still records
        FAILED (translation_error); the non-translatable one is a not_translatable skip."""
        variant_ok = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#11", hgvs_nt="NM_000000.1:c.1A>G", data={}
        )
        variant_skip = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#12", hgvs_nt="NM_000000.1:c.2G>T", data={}
        )
        variant_fail = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#13", hgvs_nt="NM_000000.1:c.3T>A", data={}
        )
        session.add_all([variant_ok, variant_skip, variant_fail])
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        c_candidate = "NM_000001.1:c.5A>G"
        construct = fake_construct(
            {"NM_000000.1:c.1A>G": [(c_candidate, None)]},
            errors_by_hgvs={"NM_000000.1:c.3T>A": "forward translation failed"},
            not_translatable_by_hgvs={"NM_000000.1:c.2G>T": "no DNA equivalence class"},
        )
        translate = fake_translate({c_candidate: "ga4gh:VA.coding"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 1, "skipped": 1, "alleles_created": 1, "fold_in_missed": 1}

        # The genuine error is FAILED with reason translation_error — distinct from the benign skip.
        failed = _cross_level_events(session, sample_score_set.id, disposition="failed")
        assert len(failed) == 1
        assert failed[0].reason == "translation_error"
        assert failed[0].event_metadata["error_message"] == "forward translation failed"

        # The non-translatable one is not_applicable / not_translatable.
        skipped = _cross_level_events(session, sample_score_set.id, reason="not_translatable")
        assert len(skipped) == 1
        assert skipped[0].disposition == "not_applicable"

    async def test_upstream_outage_is_failed_as_api_error_not_translation_error(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A UTA outage that outlasts the library's retries is FAILED with reason api_error, so it
        stays distinguishable from a genuine translation_error when auditing coverage gaps."""
        variant_ok = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#11", hgvs_nt="NM_000000.1:c.1A>G", data={}
        )
        variant_down = Variant(
            score_set_id=sample_score_set.id, urn=f"{sample_score_set.urn}#12", hgvs_nt="NM_000000.1:c.2G>T", data={}
        )
        session.add_all([variant_ok, variant_down])
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        c_candidate = "NM_000001.1:c.5A>G"
        outage = "reverse-translate-variants failed: server closed the connection unexpectedly"
        construct = fake_construct(
            {"NM_000000.1:c.1A>G": [(c_candidate, None)]},
            upstream_unavailable_by_hgvs={"NM_000000.1:c.2G>T": outage},
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", fake_translate({c_candidate: "ga4gh:VA.coding"})),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 1, "skipped": 0, "alleles_created": 1, "fold_in_missed": 1}

        failed = _cross_level_events(session, sample_score_set.id, disposition="failed")
        assert len(failed) == 1
        assert failed[0].reason == "api_error"
        assert failed[0].event_metadata["error_message"] == outage
        assert _cross_level_events(session, sample_score_set.id, reason="translation_error") == []

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
        """One member of a projection pair translating and the other failing VRS translation leaves the
        variant a SUCCESS (one allele created) while retaining the dropped candidate in metadata."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        good_coding = "NM_000001.1:c.5A>G"
        bad_genomic = "NC_000001.11:g.999A>T"
        construct = fake_construct({assay_hgvs: [(good_coding, bad_genomic)]})
        translate = fake_translate(
            {good_coding: "ga4gh:VA.coding"},
            errors_by_hgvs={bad_genomic: "untranslatable form"},
        )

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1, "fold_in_missed": 1}
        assert len(_non_authoritative_links(session)) == 1

        events = _cross_level_events(session, sample_score_set.id, disposition="present")
        assert len(events) == 1
        failed_candidates = events[0].event_metadata["failed_candidates"]
        assert failed_candidates == [{"hgvs": bad_genomic, "level": "genomic", "error": "untranslatable form"}]

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
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        bad_candidate = "NM_000001.1:c.9A>T"  # a coding-only projection pair whose sole member fails
        construct = fake_construct({assay_hgvs: [(bad_candidate, None)]})
        translate = fake_translate({}, errors_by_hgvs={bad_candidate: "untranslatable form"})

        with (
            patch(f"{RT_MODULE}.construct_equivalent_variants", construct),
            patch(f"{RT_MODULE}.translate_hgvs_to_variation", translate),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.FAILED
        assert result.data == {"translated": 0, "failed": 1, "skipped": 0, "alleles_created": 0, "fold_in_missed": 0}
        assert _non_authoritative_links(session) == []

        failed_events = _cross_level_events(session, sample_score_set.id, disposition="failed")
        assert len(failed_events) == 1
        assert failed_events[0].event_metadata["error_message"] == "All candidate HGVS failed VRS translation."
        metadata = failed_events[0].event_metadata
        assert metadata["hgvs_input"] == assay_hgvs
        assert metadata["failed_candidates"] == [
            {"hgvs": bad_candidate, "level": "cdna", "error": "untranslatable form"}
        ]

    async def test_unresolved_transcript_is_tallied_as_skip_but_recorded_as_failed(
        self,
        session,
        with_independent_processing_runs,
        with_reverse_translation_run,
        mock_worker_ctx,
        sample_independent_variant_mapping_run,
        sample_independent_reverse_translation_run,
        sample_score_set,
    ):
        """A protein-coding target aligned only at the genomic level has no resolvable coding
        transcript (transcript_unresolved). The job tallies it in its `skipped` counter, but the
        recoverable gap is recorded as a `failed` event — distinct from a hard translation
        failure (reason `translation_failed`) and from a true biological absence
        (no_coding_transcript → `absent`)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}

        # No translation attempted: no candidate alleles or links, and the variant is
        # recorded as SKIPPED rather than FAILED.
        assert _non_authoritative_links(session) == []
        # The job counts this as a skip, but a protein-coding target with no resolvable coding
        # transcript is a *recoverable* gap (transcript_unresolved) → recorded as a `failed` event,
        # distinct from a hard translation failure (which carries reason `translation_failed`).
        unresolved = _cross_level_events(session, sample_score_set.id, reason="transcript_unresolved")
        assert len(unresolved) == 1
        assert unresolved[0].disposition == "failed"

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
            urn=f"{sample_score_set.urn}#11",
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
        c_candidate = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.2000A>G"
        with (
            patch(
                f"{RT_MODULE}.construct_equivalent_variants", fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
            ),
            patch(
                f"{RT_MODULE}.translate_hgvs_to_variation",
                fake_translate({c_candidate: "ga4gh:VA.coding", g_candidate: "ga4gh:VA.genomic"}),
            ),
        ):
            result = await _reverse_translate(session, mock_worker_ctx, sample_independent_reverse_translation_run)

        assert result.status == JobStatus.SUCCEEDED
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 2, "fold_in_missed": 1}
        events = _cross_level_events(session, sample_score_set.id)
        assert events and all(e.disposition == "present" for e in events)

    def _run_mapped_date(self, session, target_gene_id):
        """The mapped_date the (mock) mapping run stamped on its TargetGeneMappings."""
        return (
            session.query(TargetGeneMapping)
            .filter(
                TargetGeneMapping.target_gene_id == target_gene_id,
                TargetGeneMapping.alignment_level == SequenceLevel.genomic,
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
        """When a run has more than one cdna TargetGeneMapping for a target (same job_run_id),
        RT reverse-translates against the newest (highest id), not an arbitrary one."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NC_000001.11:g.1000A>G",
            data={},
        )
        session.add(variant)
        session.commit()
        # Mapping emits a cdna TargetGeneMapping (NM_999999.1) stamped with this run's job_run_id.
        await _map_variants(
            session,
            mock_worker_ctx,
            sample_independent_variant_mapping_run,
            sample_score_set,
            with_layers={"g", "c"},
        )
        target_gene = sample_score_set.target_genes[0]
        run_date = self._run_mapped_date(session, target_gene.id)
        # A newer cdna row (higher id) for the same target, from the same run.
        session.add(
            TargetGeneMapping(
                target_gene_id=target_gene.id,
                alignment_level=SequenceLevel.cdna,
                reference_accession="NM_111111.1",
                preferred=False,
                tool_version="pytest.0.0",
                mapped_date=run_date,
                job_run_id=sample_independent_variant_mapping_run.id,
            )
        )
        session.commit()

        assay_hgvs = "NC_000001.11:g.1000A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.2000A>G"
        delegate = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
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
        """A leftover cdna row that does not belong to the current run is not used: the current run
        emitted no cdna row, so the variant is skipped (transcript unresolved) rather than
        reverse-translated against the stale transcript. The record resolves its transcript by its own
        run's job_run_id, so a row carrying no such anchor never leaks in (mavedb-api#763)."""
        variant = Variant(
            score_set_id=sample_score_set.id,
            urn=f"{sample_score_set.urn}#11",
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
                alignment_level=SequenceLevel.cdna,
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

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}
        skipped = _cross_level_events(session, sample_score_set.id, reason="transcript_unresolved")
        assert len(skipped) == 1
        assert skipped[0].disposition == "failed"

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
            urn=f"{sample_score_set.urn}#11",
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

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}
        skipped = _cross_level_events(session, sample_score_set.id, reason="transcript_unresolved")
        assert len(skipped) == 1
        assert skipped[0].disposition == "failed"

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
            urn=f"{sample_score_set.urn}#11",
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

        assert result.data == {"translated": 0, "failed": 0, "skipped": 1, "alleles_created": 0, "fold_in_missed": 0}
        skipped = _cross_level_events(session, sample_score_set.id, reason="no_coding_transcript")
        assert len(skipped) == 1
        assert skipped[0].disposition == "absent"

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
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        translate = fake_translate({c_candidate: "ga4gh:VA.coding", g_candidate: "ga4gh:VA.genomic"})

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
            urn=f"{sample_score_set.urn}#11",
            hgvs_nt="NM_000000.1:c.1A>G",
            hgvs_pro="NP_000000.1:p.Met1Val",
            data={},
        )
        session.add(variant)
        session.commit()
        await _map_variants(session, mock_worker_ctx, sample_independent_variant_mapping_run, sample_score_set)

        assay_hgvs = "NM_000000.1:c.1A>G"
        c_candidate = "NM_000001.1:c.5A>G"
        g_candidate = "NC_000001.11:g.1000A>G"
        delegate = fake_construct({assay_hgvs: [(c_candidate, g_candidate)]})
        translate = fake_translate({c_candidate: "ga4gh:VA.coding", g_candidate: "ga4gh:VA.genomic"})

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
            urn=f"{sample_score_set.urn}#11",
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
        c_candidate = "NM_000111.1:c.3A>G"
        translate = fake_translate({c_candidate: "ga4gh:VA.coding"})

        # Capture the transcript hint each VariantInput was built with, then delegate to the
        # normal stub for the translation result.
        captured_hints: dict[str, str | None] = {}
        delegate = fake_construct({assay_hgvs: [(c_candidate, None)]})

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
        assert result.data == {"translated": 1, "failed": 0, "skipped": 0, "alleles_created": 1, "fold_in_missed": 0}
        # The NP_ accession was resolved to its NM_ transcript and passed as the hint.
        assert captured_hints == {assay_hgvs: "NM_000111.1"}
