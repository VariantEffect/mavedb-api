# ruff: noqa: E402
"""Tests for the backfill campaign map (coverage) and safe-to-drop gate (reconcile)."""

import pytest

pytest.importorskip("arq")

from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.scripts.backfill_campaign import (
    STATE_ENRICHED,
    STATE_NONE,
    STATE_PARTIAL_RESHAPE,
    STATE_RESHAPED,
    STATE_RT,
    ScoreSetCoverage,
    classify_state,
    compute_coverage,
    compute_reconcile,
    resolve_cohort_ids,
    scan_legacy_readers,
)
from tests.helpers.constants import TEST_MINIMAL_MAPPED_VARIANT
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record


# Pure: state classification


def _cov(**kw) -> ScoreSetCoverage:
    base = dict(score_set_id=1, urn="urn:mavedb:x-1", n_variants=10, n_legacy_variants=10, n_reshaped_variants=10)
    base.update(kw)
    return ScoreSetCoverage(**base)


@pytest.mark.unit
class TestClassifyState:
    def test_none_when_no_live_record(self):
        assert classify_state(_cov(n_reshaped_variants=0), require_current_vep=False) == STATE_NONE

    def test_partial_reshape(self):
        cov = _cov(n_legacy_variants=10, n_reshaped_variants=4)
        assert classify_state(cov, require_current_vep=False) == STATE_PARTIAL_RESHAPE

    def test_reshaped_without_rt(self):
        cov = _cov(n_nonauthoritative_alleles=0)
        assert classify_state(cov, require_current_vep=False) == STATE_RESHAPED

    def test_rt_when_fanout_present_but_enrichment_incomplete(self):
        cov = _cov(n_nonauthoritative_alleles=5, n_gnomad_alleles=1)  # missing clinvar/clingen/vep
        assert classify_state(cov, require_current_vep=False) == STATE_RT

    def test_enriched_requires_all_sources(self):
        cov = _cov(
            n_nonauthoritative_alleles=5,
            n_gnomad_alleles=1,
            n_clinvar_alleles=1,
            n_clingen_alleles=1,
            n_vep_alleles=1,
        )
        assert classify_state(cov, require_current_vep=False) == STATE_ENRICHED

    def test_current_vep_gate_downgrades_when_only_stale_vep_present(self):
        cov = _cov(
            n_nonauthoritative_alleles=5,
            n_gnomad_alleles=1,
            n_clinvar_alleles=1,
            n_clingen_alleles=1,
            n_vep_alleles=1,  # some VEP, but...
            n_vep_current_alleles=0,  # ...none at the pinned current version
        )
        assert classify_state(cov, require_current_vep=True) == STATE_RT
        assert classify_state(cov, require_current_vep=False) == STATE_ENRICHED

    def test_fully_annotated_without_fanout_is_enriched(self):
        # Reverse translation can legitimately produce zero siblings (single-level score set). Such a set,
        # once fully annotated, must reach 'enriched' rather than being stranded at 'reshaped'. VEP presence
        # (VEP depends on RT) confirms RT ran even with no fan-out.
        cov = _cov(
            n_nonauthoritative_alleles=0,
            n_gnomad_alleles=1,
            n_clinvar_alleles=1,
            n_clingen_alleles=1,
            n_vep_alleles=1,
        )
        assert classify_state(cov, require_current_vep=False) == STATE_ENRICHED

    def test_vep_present_without_fanout_reads_as_rt_when_incomplete(self):
        # VEP live but a source still missing: RT ran (VEP implies it), so this is 'rt', not 'reshaped'.
        cov = _cov(n_nonauthoritative_alleles=0, n_vep_alleles=1)  # missing gnomad/clinvar/clingen
        assert classify_state(cov, require_current_vep=False) == STATE_RT

    def test_is_reshaped_needs_a_legacy_denominator(self):
        # A score set with no legacy rows can't be "reshaped" against a zero denominator.
        assert not _cov(n_legacy_variants=0, n_reshaped_variants=5).is_reshaped


# Pure: static legacy-reader scan


def _write(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.mark.unit
class TestScanLegacyReaders:
    def test_flags_a_query_reader_outside_the_allowlist(self, tmp_path):
        _write(tmp_path, "src/mavedb/routers/rogue.py", "rows = db.scalars(select(MappedVariant)).all()\n")
        hits = scan_legacy_readers(tmp_path)
        assert hits == ["src/mavedb/routers/rogue.py:1"]

    def test_flags_join_and_loader_idioms(self, tmp_path):
        _write(
            tmp_path,
            "src/mavedb/lib/x.py",
            "q = select(Variant).join(MappedVariant)\no = joinedload(Foo.bar, MappedVariant)\n",
        )
        assert scan_legacy_readers(tmp_path) == ["src/mavedb/lib/x.py:1", "src/mavedb/lib/x.py:2"]

    def test_flags_association_table_query(self, tmp_path):
        _write(
            tmp_path, "src/mavedb/lib/y.py", "n = select(func.count()).select_from(gnomad_variants_mapped_variants)\n"
        )
        assert scan_legacy_readers(tmp_path) == ["src/mavedb/lib/y.py:1"]

    def test_ignores_declarations_imports_aliases_and_prose(self, tmp_path):
        _write(
            tmp_path,
            "src/mavedb/models/variant.py",
            (
                "from mavedb.models.mapped_variant import MappedVariant\n"
                'mapped_variants: Mapped[list["MappedVariant"]] = relationship("MappedVariant")\n'
                "Alias = Union[ScoreSet, MappedVariant, Variant]\n"
                "# a comment naming MappedVariant\n"
                '"""a docstring naming MappedVariant."""\n'
                "cleanup = delete(MappedVariant)\n"  # a writer, removed with the drop — not a reader
            ),
        )
        assert scan_legacy_readers(tmp_path) == []

    def test_allowlisted_files_are_skipped(self, tmp_path):
        _write(
            tmp_path,
            "src/mavedb/scripts/export_public_data.py",
            "rows = db.scalars(select(MappedVariant)).all()\n",
        )
        assert scan_legacy_readers(tmp_path) == []


# DB-backed: coverage and reconcile


def _add_variants(session, score_set, n):
    variants = []
    for i in range(n):
        v = Variant(urn=f"{score_set.urn}#{i}", score_set_id=score_set.id, hgvs_nt=f"c.{i + 1}A>G", data={})
        session.add(v)
        variants.append(v)
    session.commit()
    return variants


def _add_legacy_mapped_variant(session, variant, *, current=True):
    session.add(MappedVariant(**{**TEST_MINIMAL_MAPPED_VARIANT, "current": current}, variant_id=variant.id))
    session.commit()


def _published(session, make_score_set, urn):
    ss = make_score_set(published=True)
    ss.urn = urn
    session.commit()
    return ss


@pytest.mark.integration
class TestComputeCoverage:
    def test_not_reshaped_score_set_is_none(self, session, make_score_set):
        ss = _published(session, make_score_set, "urn:mavedb:00000900-a-1")
        for v in _add_variants(session, ss, 3):
            _add_legacy_mapped_variant(session, v)

        cov = compute_coverage(session, [ss.id])[ss.id]
        assert cov.n_variants == 3
        assert cov.n_legacy_variants == 3
        assert cov.n_reshaped_variants == 0
        assert classify_state(cov, require_current_vep=False) == STATE_NONE

    def test_reshaped_only_score_set(self, session, make_score_set):
        ss = _published(session, make_score_set, "urn:mavedb:00000901-a-1")
        for i, v in enumerate(_add_variants(session, ss, 2)):
            _add_legacy_mapped_variant(session, v)
            seed_mapping_record(
                session,
                v,
                assay_level="genomic",
                alleles=[AlleleSpec(digest=f"resh-{i}", level="genomic", is_authoritative=True)],
            )

        cov = compute_coverage(session, [ss.id])[ss.id]
        assert cov.n_reshaped_variants == 2
        assert cov.is_reshaped
        assert not cov.has_rt
        assert classify_state(cov, require_current_vep=False) == STATE_RESHAPED

    def test_rt_and_enriched_score_set(self, session, make_score_set):
        ss = _published(session, make_score_set, "urn:mavedb:00000902-a-1")
        gnomad = GnomADVariant(
            db_name="gnomAD",
            db_identifier="1-1-A-G",
            db_version="4",
            allele_count=1,
            allele_number=2,
            allele_frequency=0.5,
        )
        control = ClinvarControl(
            db_identifier="1",
            gene_symbol="X",
            clinical_significance="Pathogenic",
            clinical_review_status="ok",
            db_name="ClinVar",
            db_version="2026-01",
        )
        session.add_all([gnomad, control])
        session.commit()

        v = _add_variants(session, ss, 1)[0]
        _add_legacy_mapped_variant(session, v)
        seed_mapping_record(
            session,
            v,
            assay_level="genomic",
            alleles=[
                AlleleSpec(digest="auth", level="genomic", is_authoritative=True, clingen_allele_id="CA1"),
                # RT fan-out sibling carrying every source annotation:
                AlleleSpec(
                    digest="sib",
                    level="cdna",
                    is_authoritative=False,
                    projection_group=1,
                    vep_consequence="missense_variant",
                    clinvar_control_ids=(control.id,),
                    gnomad_variant_ids=(gnomad.id,),
                ),
            ],
        )

        cov = compute_coverage(session, [ss.id])[ss.id]
        assert cov.has_rt
        assert cov.n_nonauthoritative_alleles == 1
        assert cov.has_gnomad and cov.has_clinvar and cov.has_clingen and cov.has_vep(require_current=False)
        assert classify_state(cov, require_current_vep=False) == STATE_ENRICHED
        # Pinning VEP to a version the seed did not write (seed writes "116") downgrades to rt.
        cov_pinned = compute_coverage(session, [ss.id], vep_source_version="999")[ss.id]
        assert cov_pinned.n_vep_current_alleles == 0
        assert classify_state(cov_pinned, require_current_vep=True) == STATE_RT
        # And pinning to the version the seed wrote counts it current again.
        cov_match = compute_coverage(session, [ss.id], vep_source_version="116")[ss.id]
        assert cov_match.n_vep_current_alleles == 1


@pytest.mark.integration
class TestComputeReconcile:
    def test_not_safe_when_a_legacy_variant_is_unreshaped(self, session, make_score_set, tmp_path):
        ss = _published(session, make_score_set, "urn:mavedb:00000903-a-1")
        for v in _add_variants(session, ss, 2):
            _add_legacy_mapped_variant(session, v)

        report = compute_reconcile(session, [ss.id], repo_root=tmp_path)
        assert report.legacy_current_variants == 2
        assert report.missing_reshape_variants == 2
        assert not report.measured_parity_ok
        assert not report.safe_to_drop

    def test_safe_when_every_legacy_variant_is_reshaped_and_readers_clear(self, session, make_score_set, tmp_path):
        ss = _published(session, make_score_set, "urn:mavedb:00000904-a-1")
        for i, v in enumerate(_add_variants(session, ss, 2)):
            _add_legacy_mapped_variant(session, v)
            seed_mapping_record(
                session,
                v,
                assay_level="genomic",
                alleles=[AlleleSpec(digest=f"ok-{i}", level="genomic", is_authoritative=True)],
            )

        report = compute_reconcile(session, [ss.id], repo_root=tmp_path)  # empty tmp repo -> no readers
        assert report.missing_reshape_variants == 0
        assert report.measured_parity_ok
        assert report.annotation_parity_ok
        assert report.readers_clear
        assert report.safe_to_drop

    def test_annotation_regression_blocks_the_drop(self, session, make_score_set, tmp_path):
        # A variant the frozen tables annotate for ClinVar, reshaped but with NO live substrate ClinVar link
        # (the protein force-close case), must block the drop until enrichment restores it.
        ss = _published(session, make_score_set, "urn:mavedb:00000906-a-1")
        control = ClinvarControl(
            db_identifier="9",
            gene_symbol="X",
            clinical_significance="Pathogenic",
            clinical_review_status="ok",
            db_name="ClinVar",
            db_version="2026-01",
        )
        session.add(control)
        session.commit()
        v = _add_variants(session, ss, 1)[0]
        # Legacy MappedVariant carrying a ClinVar association...
        mv = MappedVariant(**{**TEST_MINIMAL_MAPPED_VARIANT, "current": True}, variant_id=v.id)
        mv.clinical_controls = [control]
        session.add(mv)
        session.commit()
        # ...reshaped to the measured level, but with no live ClinVar link on the substrate.
        seed_mapping_record(
            session, v, assay_level="protein", alleles=[AlleleSpec(digest="p", level="protein", is_authoritative=True)]
        )

        report = compute_reconcile(session, [ss.id], repo_root=tmp_path)
        assert report.measured_parity_ok  # measured level is complete...
        clinvar = next(p for p in report.annotation_parity if p.source == "clinvar")
        assert clinvar.legacy_variants == 1
        assert clinvar.regressed_variants == 1  # ...but ClinVar serving would regress.
        assert not report.annotation_parity_ok
        assert not report.safe_to_drop

    def test_unexpected_reader_blocks_even_with_full_parity(self, session, make_score_set, tmp_path):
        ss = _published(session, make_score_set, "urn:mavedb:00000905-a-1")
        v = _add_variants(session, ss, 1)[0]
        _add_legacy_mapped_variant(session, v)
        seed_mapping_record(
            session, v, assay_level="genomic", alleles=[AlleleSpec(digest="ok", level="genomic", is_authoritative=True)]
        )
        _write(tmp_path, "src/mavedb/routers/rogue.py", "select(MappedVariant)\n")

        report = compute_reconcile(session, [ss.id], repo_root=tmp_path)
        assert report.measured_parity_ok
        assert not report.readers_clear
        assert not report.safe_to_drop


@pytest.mark.integration
class TestResolveCohortIds:
    def test_published_only_includes_accession_based(self, session, make_score_set):
        # The accession-based-skip pitfall: --published-only must not discriminate on target type.
        seq = make_score_set(published=True, gene_names=("BRCA1",))
        acc = make_score_set(published=True, gene_names=(), accession_gene_names=("TP53",))
        private = make_score_set(published=False, gene_names=("EGFR",))
        seq.urn, acc.urn, private.urn = "urn:mavedb:00000910-a-1", "urn:mavedb:00000911-a-1", "urn:mavedb:00000912-a-1"
        session.commit()

        ids = resolve_cohort_ids(
            session, published_only=True, include_private=False, collection_urn=None, explicit_urns=None
        )
        assert seq.id in ids
        assert acc.id in ids
        assert private.id not in ids
