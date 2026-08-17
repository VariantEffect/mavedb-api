# ruff: noqa: E402
"""Tests for the on-the-fly Cat-VRS transit builder.

The pure builder is unit-tested over transient ``MappingRecordAllele`` instances (no DB) — asserting
the mode, the member->defining relations, and the spec-pure ``CategoricalVariant`` shape for both
score-collapse modes. The DB-backed wrapper ``categorical_variant_for_variant`` is exercised at the
bottom against a real session to pin the fetch + ``as_of`` threading.
"""

from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from ga4gh.cat_vrs.models import CategoricalVariant, DefiningAlleleConstraint, Relation
from ga4gh.core.models import Relation as MappingRelation

from mavedb.lib.cat_vrs import (
    _SPEC_EQUIVALENT,
    _relation_concept,
    CatVrsMode,
    CatVrsRelation,
    build_categorical_variant,
    categorical_variant_for_variant,
)
from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_MINIMAL_VARIANT

# A spec-valid 32-char VRS digest; the internal VRS digest is irrelevant to the builder, which keys
# member relations on the `vrs_digest` *column*, so one fixed valid value across members is fine.
_VALID_DIGEST = "0123456789abcdefghijABCDEFGHIJ_-"


def _post_mapped() -> dict:
    """A minimal but spec-valid post_mapped VRS Allele dict."""
    return {
        "id": f"ga4gh:VA.{_VALID_DIGEST}",
        "type": "Allele",
        "state": {"type": "LiteralSequenceExpression", "sequence": "F"},
        "digest": _VALID_DIGEST,
        "location": {
            "id": f"ga4gh:SL.{_VALID_DIGEST}",
            "end": 6,
            "type": "SequenceLocation",
            "start": 5,
            "digest": _VALID_DIGEST,
            "sequenceReference": {
                "type": "SequenceReference",
                "label": "NP_000000.0",
                "refgetAccession": "SQ.0123456789abcdefghijABCDEFGHIJ_-",
            },
        },
    }


_DEFAULT_POST_MAPPED = object()


def _link(
    *, level: str, digest: str, is_authoritative: bool, post_mapped=_DEFAULT_POST_MAPPED, projection_group=None
) -> MappingRecordAllele:
    """A transient (record, allele) link with its allele attached — no session needed.

    ``digest`` is the ``Allele.vrs_digest`` *column* (the key the builder uses for member relations),
    deliberately distinct from the spec-valid VRS digest embedded in post_mapped. Pass
    ``post_mapped=None`` to model an un-hydratable allele. ``projection_group`` pairs a c↔g projection
    (the two links of one precise change share a value; the protein apex carries ``None``); the builder
    uses it in projection mode to keep only the measured change's precise coordinate partner.
    """
    pm = _post_mapped() if post_mapped is _DEFAULT_POST_MAPPED else post_mapped
    allele = Allele(level=level, vrs_digest=digest, post_mapped=pm)
    return MappingRecordAllele(is_authoritative=is_authoritative, allele=allele, projection_group=projection_group)


@pytest.mark.unit
def test_mode_2_protein_measured_reverse_translation():
    """Protein measured: defining is the protein allele; both nt members `encodes` it (star model)."""
    links = [
        _link(level="protein", digest="prot", is_authoritative=True),
        _link(level="cdna", digest="cdna", is_authoritative=False),
        _link(level="genomic", digest="gen", is_authoritative=False),
    ]

    transit = build_categorical_variant(links, name="urn:mavedb:test#1")
    assert transit is not None

    assert transit.mode == CatVrsMode.REVERSE_TRANSLATION
    # Per-member relations exclude the defining allele; both nt siblings encode the protein.
    assert transit.member_relations == {
        "cdna": CatVrsRelation.ENCODES,
        "gen": CatVrsRelation.ENCODES,
    }

    cv = transit.categorical_variant
    assert isinstance(cv, CategoricalVariant)
    # Every representation is a member, including the defining protein allele.
    assert len(cv.members) == 3
    # Constraints come back as the `Constraint` union RootModel; unwrap to the concrete type.
    constraint = cv.constraints[0].root
    assert isinstance(constraint, DefiningAlleleConstraint)
    # Relations on the constraint carry only the distinct kinds present (here: just `encodes`).
    assert [str(r.primaryCoding.code.root) for r in constraint.relations] == ["encodes"]


@pytest.mark.unit
def test_mode_1_coding_measured_projection():
    """Coding measured: the projection_group partner is a coordinate representation; protein is a translation."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0),
        _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0),
        _link(level="protein", digest="prot", is_authoritative=False),
    ]

    transit = build_categorical_variant(links, name="urn:mavedb:test#2")
    assert transit is not None

    assert transit.mode == CatVrsMode.PROJECTION
    assert transit.member_relations == {
        "gen": CatVrsRelation.COORDINATE_REPRESENTATION_OF,
        "prot": CatVrsRelation.TRANSLATION_OF,
    }

    constraint = transit.categorical_variant.constraints[0].root
    codes = {str(r.primaryCoding.code.root) for r in constraint.relations}
    assert codes == {"coordinate_representation_of", "translation_of"}


@pytest.mark.unit
def test_mode_1_projection_includes_sibling_encoders():
    """Coding measured, full closure (default include_convergent=True): the reverse-translation fan on the
    record (other encoders of the same protein consequence, in *different* projection groups) is kept as
    members wearing the ``co_encodes`` relation, so the object is the full closure. The measured change's
    own coordinate partner and protein consequence stay coordinate_representation_of / translation_of."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0),
        _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0),
        # A sibling encoder of the same protein change — a distinct variant, different projection group.
        _link(level="cdna", digest="sibling_cdna", is_authoritative=False, projection_group=1),
        _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1),
        _link(level="protein", digest="prot", is_authoritative=False),
    ]

    transit = build_categorical_variant(links, name="urn:mavedb:test#2b")
    assert transit is not None

    # The coordinate partner + protein consequence keep their faithful relations; the two cousins ride as
    # co_encodes (distinct, unmeasured synonymous variants).
    assert transit.member_relations == {
        "gen": CatVrsRelation.COORDINATE_REPRESENTATION_OF,
        "prot": CatVrsRelation.TRANSLATION_OF,
        "sibling_cdna": CatVrsRelation.CO_ENCODES,
        "sibling_gen": CatVrsRelation.CO_ENCODES,
    }
    # members = defining cdna + gen partner + protein apex + the two cousins (full closure).
    assert len(transit.categorical_variant.members) == 5
    # The distinct relation kinds surface on the constraint, including co_encodes.
    constraint = transit.categorical_variant.constraints[0].root
    codes = {str(r.primaryCoding.code.root) for r in constraint.relations}
    assert codes == {"coordinate_representation_of", "translation_of", "co_encodes"}


@pytest.mark.unit
def test_mode_1_projection_narrow_object_drops_sibling_encoders():
    """Coding measured, narrow object (include_convergent=False, the VA subject): the synonymous cousins
    in other projection groups are dropped — only the measured change's coordinate partner and protein
    consequence remain. This pins the shape the VA-Spec path builds."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0),
        _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0),
        _link(level="cdna", digest="sibling_cdna", is_authoritative=False, projection_group=1),
        _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1),
        _link(level="protein", digest="prot", is_authoritative=False),
    ]

    transit = build_categorical_variant(links, name="urn:mavedb:test#2b-narrow", include_convergent=False)
    assert transit is not None

    # Only the measured change's coordinate partner + the protein consequence; the cousins are excluded.
    assert transit.member_relations == {
        "gen": CatVrsRelation.COORDINATE_REPRESENTATION_OF,
        "prot": CatVrsRelation.TRANSLATION_OF,
    }
    # members = defining cdna + gen partner + protein apex (the two cousins are dropped).
    assert len(transit.categorical_variant.members) == 3


@pytest.mark.unit
def test_mode_2_reverse_translation_keeps_the_full_encoder_class():
    """Protein measured: every nt encoder stays, across projection groups — the full equivalence class
    is what the protein claim ranges over (no sibling filtering in reverse-translation mode)."""
    links = [
        _link(level="protein", digest="prot", is_authoritative=True),
        _link(level="cdna", digest="cdna_a", is_authoritative=False, projection_group=0),
        _link(level="genomic", digest="gen_a", is_authoritative=False, projection_group=0),
        _link(level="cdna", digest="cdna_b", is_authoritative=False, projection_group=1),
        _link(level="genomic", digest="gen_b", is_authoritative=False, projection_group=1),
    ]

    transit = build_categorical_variant(links, name="urn:mavedb:test#2c")
    assert transit is not None

    assert transit.mode == CatVrsMode.REVERSE_TRANSLATION
    # All four nt encoders `encodes` the defining protein; none dropped.
    assert transit.member_relations == {
        "cdna_a": CatVrsRelation.ENCODES,
        "gen_a": CatVrsRelation.ENCODES,
        "cdna_b": CatVrsRelation.ENCODES,
        "gen_b": CatVrsRelation.ENCODES,
    }
    assert len(transit.categorical_variant.members) == 5


@pytest.mark.unit
def test_no_authoritative_link_returns_none():
    """An unmapped variant (no authoritative link) has no defining allele to anchor on."""
    links = [_link(level="genomic", digest="gen", is_authoritative=False)]
    assert build_categorical_variant(links, name="urn:mavedb:test#3") is None


@pytest.mark.unit
def test_empty_links_returns_none():
    assert build_categorical_variant([], name="urn:mavedb:test#4") is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "defining_level, expected_mode",
    [
        ("protein", CatVrsMode.REVERSE_TRANSLATION),
        ("cdna", CatVrsMode.PROJECTION),
        ("genomic", CatVrsMode.PROJECTION),
    ],
)
def test_mode_follows_defining_level(defining_level, expected_mode):
    links = [_link(level=defining_level, digest="d", is_authoritative=True)]
    transit = build_categorical_variant(links, name="urn:mavedb:test#5")
    assert transit is not None
    assert transit.mode == expected_mode


@pytest.mark.unit
def test_unhydratable_defining_allele_returns_none():
    """Defensive: an authoritative allele with no post_mapped can't anchor a Cat-VRS, so build → None
    (a broken invariant in practice — the mapping job writes post_mapped on the authoritative allele)."""
    links = [
        _link(level="protein", digest="prot", is_authoritative=True, post_mapped=None),
        _link(level="cdna", digest="cdna", is_authoritative=False),
    ]
    assert build_categorical_variant(links, name="urn:mavedb:test#6") is None


@pytest.mark.unit
def test_unhydratable_member_allele_is_skipped():
    """Defensive: a member with no post_mapped is dropped; the build still succeeds on the rest."""
    links = [
        _link(level="protein", digest="prot", is_authoritative=True),
        _link(level="cdna", digest="good", is_authoritative=False),
        _link(level="genomic", digest="bad", is_authoritative=False, post_mapped=None),
    ]
    transit = build_categorical_variant(links, name="urn:mavedb:test#7")

    assert transit is not None
    # The un-hydratable genomic member is excluded from both the members and the relation map.
    assert transit.member_relations == {"good": CatVrsRelation.ENCODES}
    assert len(transit.categorical_variant.members) == 2


# --- DB-backed wrapper: categorical_variant_for_variant (fetch + build, as_of threaded) ---

T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)


def _db_allele(session, digest, level):
    allele = Allele(vrs_digest=digest, level=level, post_mapped=_post_mapped())
    session.add(allele)
    session.commit()
    return allele


def _db_variant(session, score_set, suffix):
    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#{suffix}", score_set_id=score_set.id)
    session.add(variant)
    session.commit()
    return variant


def _db_record(session, variant, *, assay_level, valid_from=None):
    record = MappingRecord(
        variant_id=variant.id, assay_level=assay_level, mapping_api_version="test.0.0", valid_from=valid_from
    )
    session.add(record)
    session.commit()
    return record


def _db_link(session, record, allele, *, is_authoritative=False, valid_from=None):
    link = MappingRecordAllele(
        mapping_record_id=record.id, allele_id=allele.id, is_authoritative=is_authoritative, valid_from=valid_from
    )
    session.add(link)
    session.commit()
    return link


@pytest.mark.integration
def test_wrapper_builds_transit_from_live_links(session, setup_lib_db_with_score_set):
    """Fetch + build: a protein-measured variant's record yields a Mode 2 transit with nt `encodes`."""
    variant = _db_variant(session, setup_lib_db_with_score_set, 1)
    record = _db_record(session, variant, assay_level="protein")
    _db_link(session, record, _db_allele(session, "prot", "protein"), is_authoritative=True)
    _db_link(session, record, _db_allele(session, "cdna", "cdna"))

    transit = categorical_variant_for_variant(session, variant.id, name=variant.urn)

    assert transit is not None
    assert transit.mode == CatVrsMode.REVERSE_TRANSLATION
    assert transit.member_relations == {"cdna": CatVrsRelation.ENCODES}


@pytest.mark.integration
def test_wrapper_returns_none_for_unmapped_variant(session, setup_lib_db_with_score_set):
    variant = _db_variant(session, setup_lib_db_with_score_set, 1)
    assert categorical_variant_for_variant(session, variant.id, name=variant.urn) is None


@pytest.mark.integration
def test_wrapper_threads_as_of_to_the_historical_record(session, setup_lib_db_with_score_set):
    """as_of selects the past record, changing the built object — Mode 2 then, Mode 1 now."""
    variant = _db_variant(session, setup_lib_db_with_score_set, 1)

    old_record = _db_record(session, variant, assay_level="protein", valid_from=T0)
    _db_link(session, old_record, _db_allele(session, "old-prot", "protein"), is_authoritative=True, valid_from=T0)
    old_record.retire(session, at=T1)
    session.commit()

    new_record = _db_record(session, variant, assay_level="genomic", valid_from=T1)
    _db_link(session, new_record, _db_allele(session, "new-gen", "genomic"), is_authoritative=True, valid_from=T1)

    past = categorical_variant_for_variant(session, variant.id, name=variant.urn, as_of=T0)
    current = categorical_variant_for_variant(session, variant.id, name=variant.urn)

    assert past is not None and past.mode == CatVrsMode.REVERSE_TRANSLATION
    assert current is not None and current.mode == CatVrsMode.PROJECTION


# ---------------------------------------------------------------------------
# Spec alignment
#
# MaveDB maintains its own relation vocabulary (Cat-VRS's `Relation` is an enum with members, so it
# cannot be subclassed, and inheriting would buy nothing — interop rides on `Coding.system`, not on
# Python enum identity). These tests are the drift guard that keeps the vocabulary honest: they fail
# when the spec gains a term MaveDB has not triaged, or when MaveDB coins a term without recording
# whether the spec already covers it.
# ---------------------------------------------------------------------------


def test_mapped_relations_emit_an_exact_match_to_the_spec_term():
    """A code with a spec equivalent must carry a machine-readable mapping to it.

    A matching code *string* is not enough: the two codings sit in different systems, so nothing but an
    explicit ConceptMapping tells a consumer they are the same concept.
    """
    concept = _relation_concept(CatVrsRelation.TRANSLATION_OF)

    assert concept.primaryCoding is not None
    assert concept.primaryCoding.system == "https://mavedb.org/cat-vrs/relations"
    assert concept.mappings is not None and len(concept.mappings) == 1

    mapping = concept.mappings[0]
    assert mapping.relation == MappingRelation.EXACT_MATCH
    assert mapping.coding.code.root == Relation.TRANSLATION_OF.value
    assert mapping.coding.system != concept.primaryCoding.system


@pytest.mark.parametrize(
    "relation",
    [CatVrsRelation.ENCODES, CatVrsRelation.CO_ENCODES, CatVrsRelation.COORDINATE_REPRESENTATION_OF],
)
def test_unmapped_relations_carry_no_mapping(relation):
    """The absence of a mapping is the interoperable statement that no spec term covers this code.

    Emitting a `closeMatch` to something approximate would be worse than silence — a consumer would
    resolve it and be wrong. See `_SPEC_EQUIVALENT` for why each of these three has no equivalent.
    """
    concept = _relation_concept(relation)

    assert concept.primaryCoding is not None and concept.primaryCoding.code.root == relation.value
    assert concept.mappings is None


def test_every_relation_is_either_mapped_or_a_declared_gap():
    """No MaveDB relation may exist without a triage decision recorded in `_SPEC_EQUIVALENT`.

    Coining a new code is fine; coining one *without deciding whether the spec already covers it* is the
    drift this guards against. A new member fails here until it is either mapped or listed below.
    """
    declared_gaps = {
        CatVrsRelation.ENCODES,
        CatVrsRelation.CO_ENCODES,
        CatVrsRelation.COORDINATE_REPRESENTATION_OF,
    }

    assert set(CatVrsRelation) == set(_SPEC_EQUIVALENT) | declared_gaps


def test_spec_relations_maveDB_deliberately_never_emits():
    """Fails when Cat-VRS publishes a relation MaveDB has not triaged.

    `liftover_to` is assembly-to-assembly, which MaveDB does not do. `transcribed_to` is genomic->
    transcript only and asserts transcription; MaveDB's c<->g relation is direction-neutral coordinate
    equivalence that also holds for intronic/UTR-offset positions, so it is deliberately unmapped.
    """
    emitted = {spec_term.value for spec_term in _SPEC_EQUIVALENT.values()}
    never_emitted = {"liftover_to", "transcribed_to"}

    assert {relation.value for relation in Relation} == emitted | never_emitted
