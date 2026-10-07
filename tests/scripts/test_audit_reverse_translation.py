# ruff: noqa: E402

from datetime import datetime, timezone

import pytest

pytest.importorskip("ga4gh.vrs")

from mavedb.lib.seqrepo import SequenceNotFoundError
from sqlalchemy import select

from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.scripts.audit_reverse_translation import (
    ERROR,
    SQL_CHECKS,
    ProjectionPair,
    check_refgets_against_seqrepo,
    collect_outcomes,
    round_trip_pair,
    run_sql_check,
)
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record

CHECKS = {check.name: check for check in SQL_CHECKS}


def _post_mapped(refget: str) -> dict:
    return {
        "type": "Allele",
        "location": {"type": "SequenceLocation", "sequenceReference": {"refgetAccession": refget}},
    }


def _score_set_with_variants(session, make_score_set, urn, n):
    score_set = make_score_set()
    score_set.urn = urn
    session.commit()

    variants = [
        Variant(urn=f"{urn}#{i}", score_set_id=score_set.id, hgvs_nt=f"c.{i}A>G", data={}) for i in range(1, n + 1)
    ]
    session.add_all(variants)
    session.commit()
    return score_set, variants


def _healthy_record(session, variant, n):
    """A measured cdna allele folded into its projection pair, with the genomic twin and a protein apex."""
    return seed_mapping_record(
        session,
        variant,
        alleles=[
            AlleleSpec(
                digest=f"c-{n}",
                level="cdna",
                is_authoritative=True,
                hgvs_c=f"NM_000001.1:c.{n}A>G",
                clingen_allele_id=f"CA{n}",
                projection_group=1,
                post_mapped=_post_mapped("SQ.cdna"),
            ),
            AlleleSpec(
                digest=f"g-{n}",
                level="genomic",
                hgvs_g=f"NC_000001.11:g.{1000 + n}A>G",
                clingen_allele_id=f"CA{n}",
                projection_group=1,
                post_mapped=_post_mapped("SQ.genome"),
            ),
            AlleleSpec(
                digest=f"p-{n}",
                level="protein",
                hgvs_p=f"NP_000001.1:p.Lys{n}Glu",
                post_mapped=_post_mapped("SQ.protein"),
            ),
        ],
    )


def _count(session, name, score_set_ids):
    return run_sql_check(session, CHECKS[name], score_set_ids, samples=5)


@pytest.mark.integration
class TestSqlChecks:
    def test_a_healthy_graph_passes_every_error_check(self, session, make_score_set):
        score_set, variants = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000001-a-1", 2)
        for n, variant in enumerate(variants, start=1):
            _healthy_record(session, variant, n)

        failing = {
            check.name: finding.count
            for check in SQL_CHECKS
            if check.severity == ERROR and (finding := run_sql_check(session, check, [score_set.id], 5)).count
        }

        assert failing == {}

    def test_flags_a_measured_allele_left_outside_its_projection_group(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000002-a-1", 1)
        seed_mapping_record(
            session,
            variant,
            alleles=[
                AlleleSpec(digest="measured", level="cdna", is_authoritative=True, hgvs_c="NM_000002.1:c.5A>G"),
                AlleleSpec(digest="rt-c", level="cdna", hgvs_c="NM_000002.2:c.5A>G", projection_group=1),
                AlleleSpec(digest="rt-g", level="genomic", hgvs_g="NC_000002.12:g.50A>G", projection_group=1),
            ],
        )

        finding = _count(session, "measured_allele_outside_projection_group", [score_set.id])

        assert finding.count == 1
        assert finding.samples[0]["hgvs"] == "NM_000002.1:c.5A>G"

    def test_reports_a_measured_indel_outside_the_groups_as_info_not_error(self, session, make_score_set):
        # A deletion straddling codons 261/262 changes the protein as deleting codon 262 does, but is a
        # different DNA change, so RT's codon-aligned candidates do not include it.
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000011-a-1", 1)
        deletion = {
            "type": "Allele",
            "location": {"type": "SequenceLocation", "start": 67611951, "end": 67611954},
            "state": {"type": "LiteralSequenceExpression", "sequence": ""},
        }
        seed_mapping_record(
            session,
            variant,
            alleles=[
                AlleleSpec(
                    digest="measured",
                    level="genomic",
                    is_authoritative=True,
                    hgvs_g="NC_000016.10:g.67611952_67611954del",
                    post_mapped=deletion,
                ),
                AlleleSpec(digest="rt-c", level="cdna", hgvs_c="NM_006565.4:c.784_786del", projection_group=0),
                AlleleSpec(
                    digest="rt-g", level="genomic", hgvs_g="NC_000016.10:g.67611953_67611955del", projection_group=0
                ),
            ],
        )

        assert _count(session, "measured_allele_outside_projection_group", [score_set.id]).count == 0
        assert _count(session, "measured_indel_outside_projection_group", [score_set.id]).count == 1

    def test_flags_an_accession_whose_alleles_carry_two_refgets(self, session, make_score_set):
        score_set, variants = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000003-a-1", 2)
        for n, (variant, refget) in enumerate(zip(variants, ("SQ.ncbi", "SQ.cdot")), start=1):
            seed_mapping_record(
                session,
                variant,
                alleles=[
                    AlleleSpec(
                        digest=f"twin-{n}",
                        level="cdna",
                        is_authoritative=True,
                        hgvs_c=f"NM_007294.3:c.{n}A>G",
                        post_mapped=_post_mapped(refget),
                    )
                ],
            )

        finding = _count(session, "accession_with_multiple_refgets", [score_set.id])

        assert finding.count == 1
        assert finding.samples[0]["accession"] == "NM_007294.3"
        assert sorted(finding.samples[0]["refgets"]) == ["SQ.cdot", "SQ.ncbi"]

    @pytest.mark.parametrize(
        "hgvs, flagged",
        [
            (("NP_000004.1:p.Lys1Glu", "NP_000004.1:p.Lys1Glu="), 1),
            (("NP_000004.1:p.Lys1Glu", "NP_000004.2:p.Lys1Glu"), 0),
        ],
        ids=["same accession", "different accessions"],
    )
    def test_flags_one_clingen_allele_id_only_on_twins_on_one_accession(self, session, make_score_set, hgvs, flagged):
        score_set, variants = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000004-a-1", 2)
        for n, (variant, expression) in enumerate(zip(variants, hgvs), start=1):
            seed_mapping_record(
                session,
                variant,
                alleles=[
                    AlleleSpec(
                        digest=f"caid-{n}",
                        level="protein",
                        is_authoritative=True,
                        hgvs_p=expression,
                        clingen_allele_id="PA999",
                    )
                ],
            )

        assert _count(session, "clingen_allele_id_on_twin_alleles", [score_set.id]).count == flagged

    def test_reports_a_twin_on_a_retired_allele_as_info_not_error(self, session, make_score_set):
        score_set, variants = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000010-a-1", 2)
        records = [
            seed_mapping_record(
                session,
                variant,
                alleles=[
                    AlleleSpec(
                        digest=f"caid-{n}",
                        level="genomic",
                        is_authoritative=True,
                        hgvs_g=expression,
                        clingen_allele_id="CA999",
                    )
                ],
            )
            for n, (variant, expression) in enumerate(
                zip(variants, ("NC_000007.14:g.3043839CA=", "NC_000007.14:g.2947765_2947766delinsCA")), start=1
            )
        ]
        retired_at = datetime.now(timezone.utc)
        records[0].valid_to = retired_at
        for link in session.scalars(
            select(MappingRecordAllele).where(MappingRecordAllele.mapping_record_id == records[0].id)
        ):
            link.valid_to = retired_at
        session.commit()

        assert _count(session, "clingen_allele_id_on_twin_alleles", [score_set.id]).count == 0
        assert _count(session, "clingen_allele_id_on_retired_twin", [score_set.id]).count == 1

    def test_flags_a_projection_group_with_one_member(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000005-a-1", 1)
        seed_mapping_record(
            session,
            variant,
            alleles=[
                AlleleSpec(
                    digest="lonely",
                    level="cdna",
                    is_authoritative=True,
                    hgvs_c="NM_000005.1:c.1A>G",
                    projection_group=1,
                )
            ],
        )

        finding = _count(session, "malformed_projection_group", [score_set.id])

        assert finding.count == 1
        assert finding.samples[0]["levels"] == "cdna"

    def test_flags_a_level_that_disagrees_with_the_hgvs(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000006-a-1", 1)
        seed_mapping_record(
            session,
            variant,
            alleles=[AlleleSpec(digest="mislevel", level="cdna", is_authoritative=True, hgvs_g="NC_000006.12:g.1A>G")],
        )

        assert _count(session, "level_disagrees_with_hgvs", [score_set.id]).count == 1

    def test_flags_a_live_link_on_a_retired_record(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000007-a-1", 1)
        record = _healthy_record(session, variant, 7)
        record.valid_to = datetime.now(timezone.utc)
        session.commit()

        assert _count(session, "live_link_on_retired_record", [score_set.id]).count == 3

    def test_flags_a_mapped_record_with_no_authoritative_allele(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000008-a-1", 1)
        seed_mapping_record(session, variant, alleles=[])
        session.add(
            AnnotationEvent(
                annotation_type="vrs_mapping",
                variant_id=variant.id,
                score_set_id=score_set.id,
                disposition="present",
                reason="mapped",
            )
        )
        session.commit()

        assert _count(session, "mapped_record_without_authoritative_link", [score_set.id]).count == 1

    def test_does_not_flag_a_record_whose_mapping_failed(self, session, make_score_set):
        score_set, (variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000009-a-1", 1)
        seed_mapping_record(session, variant, alleles=[])
        session.add(
            AnnotationEvent(
                annotation_type="vrs_mapping",
                variant_id=variant.id,
                score_set_id=score_set.id,
                disposition="failed",
                reason="failed",
            )
        )
        session.commit()

        assert _count(session, "mapped_record_without_authoritative_link", [score_set.id]).count == 0

    def test_scoping_excludes_other_score_sets(self, session, make_score_set):
        clean, (clean_variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000010-a-1", 1)
        _, (broken_variant,) = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000011-a-1", 1)
        _healthy_record(session, clean_variant, 10)
        seed_mapping_record(
            session,
            broken_variant,
            alleles=[
                AlleleSpec(
                    digest="b-lonely",
                    level="cdna",
                    is_authoritative=True,
                    hgvs_c="NM_000011.1:c.1A>G",
                    projection_group=1,
                )
            ],
        )

        assert _count(session, "malformed_projection_group", [clean.id]).count == 0
        assert _count(session, "malformed_projection_group", None).count == 1


@pytest.mark.integration
class TestRefgetsAgainstSeqRepo:
    def test_counts_alleles_off_the_canonical_refget_and_lists_unresolved_accessions(self, session, make_score_set):
        score_set, variants = _score_set_with_variants(session, make_score_set, "urn:mavedb:00000012-a-1", 3)
        specs = [
            ("NM_000012.1:c.1A>G", "SQ.right"),
            ("NM_000012.1:c.2A>G", "SQ.wrong"),
            ("NM_999999.1:c.1A>G", "SQ.unknown"),
        ]
        for n, (variant, (hgvs, refget)) in enumerate(zip(variants, specs), start=1):
            seed_mapping_record(
                session,
                variant,
                alleles=[
                    AlleleSpec(
                        digest=f"sr-{n}",
                        level="cdna",
                        is_authoritative=True,
                        hgvs_c=hgvs,
                        post_mapped=_post_mapped(refget),
                    )
                ],
            )

        def resolve(accession):
            if accession == "NM_000012.1":
                return "SQ.right"
            raise SequenceNotFoundError(accession)

        mismatch, unresolved = check_refgets_against_seqrepo(session, resolve, [score_set.id], samples=5)

        assert mismatch.count == 1
        assert mismatch.samples == [
            {"accession": "NM_000012.1", "stored": "SQ.wrong", "seqrepo": "SQ.right", "alleles": 1}
        ]
        assert unresolved.count == 1
        assert unresolved.samples[0]["accession"] == "NM_999999.1"


def _pair(**overrides):
    values = dict(
        mapping_record_id=1,
        projection_group=1,
        hgvs_c="NM_1.1:c.1A>G",
        cdna_digest="c",
        hgvs_g="NC_1.11:g.100A>G",
        genomic_digest="g",
        protein_digests=["p"],
        protein_hgvs=["NP_1.1:p.Lys1Glu"],
    )
    return ProjectionPair(**{**values, **overrides})


DIGESTS = {"NC_1.11:g.100A>G": "g", "NP_1.1:p.Lys1Glu": "p", "NC_1.11:g.200A>G": "g-other"}


class TestRoundTripPair:
    def test_reports_matches_when_both_sides_identify_as_the_stored_alleles(self):
        outcomes = round_trip_pair(
            _pair(), lambda c: "NC_1.11:g.100A>G", lambda c: "NP_1.1:p.(Lys1Glu)", DIGESTS.__getitem__
        )

        assert [outcome for outcome, _ in outcomes] == ["genomic: matches", "protein: matches"]

    def test_reports_a_genomic_projection_that_identifies_differently(self):
        outcomes = round_trip_pair(
            _pair(), lambda c: "NC_1.11:g.200A>G", lambda c: "NP_1.1:p.Lys1Glu", DIGESTS.__getitem__
        )

        assert outcomes[0][0] == "GENOMIC: DISAGREES"
        assert outcomes[0][1]["derived"] == "NC_1.11:g.200A>G"

    def test_reports_a_protein_consequence_that_identifies_differently(self):
        outcomes = round_trip_pair(
            _pair(protein_digests=["p-other"]),
            lambda c: "NC_1.11:g.100A>G",
            lambda c: "NP_1.1:p.Lys1Glu",
            DIGESTS.__getitem__,
        )

        assert outcomes[1][0] == "PROTEIN: DISAGREES"

    def test_separates_translation_failures_from_disagreements(self):
        def fail(_):
            raise ValueError("intronic")

        outcomes = round_trip_pair(_pair(), fail, fail, DIGESTS.__getitem__)

        assert [outcome for outcome, _ in outcomes] == ["genomic: could not re-derive", "protein: could not re-derive"]

    @pytest.mark.parametrize("derived", ["NP_1.1:p.?", "NP_1.1:p.(Met1?)"])
    def test_reports_a_start_codon_change_as_unknown_effect_not_a_failure(self, derived):
        outcomes = round_trip_pair(_pair(), lambda c: "NC_1.11:g.100A>G", lambda c: derived, DIGESTS.__getitem__)

        assert outcomes[1][0] == "protein: unknown effect (start codon)"

    def test_two_notations_for_an_unchanged_protein_match(self):
        pair = _pair(protein_digests=["p-synonymous"], protein_hgvs=["NP_1.1:p.Thr328="])
        digests = {**DIGESTS, "NP_1.1:p.Ter330Ter": "p-stop"}

        outcomes = round_trip_pair(
            pair, lambda c: "NC_1.11:g.100A>G", lambda c: "NP_1.1:p.Ter330Ter", digests.__getitem__
        )

        assert outcomes[1][0] == "protein: matches (unchanged)"

    def test_reports_a_record_with_no_protein_apex(self):
        outcomes = round_trip_pair(
            _pair(protein_digests=[], protein_hgvs=[]), lambda c: "NC_1.11:g.100A>G", lambda c: "", DIGESTS.__getitem__
        )

        assert outcomes[1][0] == "protein: no apex on record"


class TestCollectOutcomes:
    def test_orders_score_sets_by_failure_share_and_tallies_failure_reasons(self):
        outcomes = collect_outcomes(
            [
                ("urn:a", "present", "translated", 90),
                ("urn:a", "failed", "translation_error", 10),
                ("urn:b", "present", "translated", 50),
                ("urn:b", "failed", "translation_failed", 30),
                ("urn:b", "failed", "translation_error", 20),
            ]
        )

        assert [o.urn for o in outcomes] == ["urn:b", "urn:a"]
        assert outcomes[0].failed_pct == 50.0
        assert outcomes[0].failure_reasons.most_common(1) == [("translation_failed", 30)]
        assert outcomes[1].total == 100
