# ruff: noqa: E402
"""Tests for the on-the-fly Cat-VRS transit builder.

The pure builder is unit-tested over transient ``MappingRecordAllele`` instances (no DB) — asserting
the mode, the member->defining relations, and the spec-pure ``CategoricalVariant`` shape for both
score-collapse modes, and the external cross-references carried in ``mappings``.
"""

import pytest

pytest.importorskip("psycopg2")

from ga4gh.cat_vrs.models import CategoricalVariant, DefiningAlleleConstraint
from ga4gh.cat_vrs.relations import Relation
from ga4gh.core.models import Relation as MappingRelation

from mavedb.lib.cat_vrs import (
    _SPEC_EQUIVALENT,
    _relation_concept,
    CatVrsMode,
    CatVrsRelation,
    build_categorical_variant,
    categorical_member_links,
)
from mavedb.lib.allele_annotations import AlleleCrossReferences, GnomadReference
from mavedb.models.allele import Allele
from mavedb.models.mapping_record_allele import MappingRecordAllele

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
    *,
    level: str,
    digest: str,
    is_authoritative: bool,
    post_mapped=_DEFAULT_POST_MAPPED,
    projection_group=None,
    caid=None,
) -> MappingRecordAllele:
    """A transient (record, allele) link with its allele attached — no session needed.

    ``digest`` is the ``Allele.vrs_digest`` *column* (the key the builder uses for member relations),
    deliberately distinct from the spec-valid VRS digest embedded in post_mapped. Pass
    ``post_mapped=None`` to model an un-hydratable allele. ``projection_group`` pairs a c↔g projection
    (the two links of one precise change share a value; the protein apex carries ``None``); the builder
    uses it in projection mode to keep only the measured change's precise coordinate partner. ``caid`` is the
    allele's ClinGen id, which the builder cross-references in ``mappings``.
    """
    pm = _post_mapped() if post_mapped is _DEFAULT_POST_MAPPED else post_mapped
    allele = Allele(level=level, vrs_digest=digest, post_mapped=pm, clingen_allele_id=caid)
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


# --- External cross-references: CategoricalVariant.mappings ---

_CLINGEN = "https://reg.clinicalgenome.org/"
_GNOMAD = "https://gnomad.broadinstitute.org"
_CLINVAR = "https://www.ncbi.nlm.nih.gov/clinvar/variation/"


def _gnomad(db_identifier: str, db_version: str = "v4.1") -> AlleleCrossReferences:
    return AlleleCrossReferences(gnomad=GnomadReference(db_identifier=db_identifier, db_version=db_version))


def _clinvar(*variation_ids: str) -> AlleleCrossReferences:
    """One ClinVar variation ID per live release; a repeated ID models one variation across releases."""
    return AlleleCrossReferences(clinvar_variation_ids=list(variation_ids))


def _mappings(transit) -> dict[tuple[str, str], MappingRelation]:
    """(system, code) -> relation, for the categorical variant's external cross-references."""
    return {(m.coding.system, m.coding.code.root): m.relation for m in transit.categorical_variant.mappings or []}


@pytest.mark.unit
def test_projection_mode_maps_the_measured_change_as_exact_and_other_variants_as_related():
    """The measured change and its coordinate twin are the categorical variant (exactMatch); the protein
    consequence and convergent encodings are distinct variants (relatedMatch). The twins' shared CAID and
    a ClinVar variation repeated across releases each collapse to one mapping."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0, caid="CA1"),
        _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0, caid="CA1"),
        _link(level="protein", digest="prot", is_authoritative=False, caid="PA1"),
        _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1, caid="CA2"),
    ]
    references = {
        "cdna": _clinvar("100", "100"),
        "gen": _gnomad("1-100-A-G"),
        "sibling_gen": _gnomad("1-101-C-T"),
    }

    transit = build_categorical_variant(links, name="urn:mavedb:test#map1", cross_references=references)

    assert transit is not None
    assert _mappings(transit) == {
        (_CLINGEN, "CA1"): MappingRelation.EXACT_MATCH,
        (_CLINVAR, "100"): MappingRelation.EXACT_MATCH,
        (_GNOMAD, "1-100-A-G"): MappingRelation.EXACT_MATCH,
        (_CLINGEN, "PA1"): MappingRelation.RELATED_MATCH,
        (_CLINGEN, "CA2"): MappingRelation.RELATED_MATCH,
        (_GNOMAD, "1-101-C-T"): MappingRelation.RELATED_MATCH,
    }


@pytest.mark.unit
def test_narrow_object_omits_mappings_for_dropped_convergent_encodings():
    """Mappings follow the member set: the VA subject drops convergent encodings, so their records go too."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0),
        _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1),
    ]
    references = {"cdna": _gnomad("1-100-A-G"), "sibling_gen": _gnomad("1-101-C-T")}

    transit = build_categorical_variant(
        links, name="urn:mavedb:test#map2", include_convergent=False, cross_references=references
    )

    assert transit is not None
    assert _mappings(transit) == {(_GNOMAD, "1-100-A-G"): MappingRelation.EXACT_MATCH}


@pytest.mark.unit
def test_reverse_translation_maps_each_encoding_as_related():
    """Protein measured: the protein's own CAID is exact; every nt encoding is a distinct variant."""
    links = [
        _link(level="protein", digest="prot", is_authoritative=True, caid="PA1"),
        _link(level="cdna", digest="cdna_a", is_authoritative=False, projection_group=0, caid="CA1"),
        _link(level="cdna", digest="cdna_b", is_authoritative=False, projection_group=1, caid="CA2"),
    ]
    references = {"cdna_a": _clinvar("100"), "cdna_b": _clinvar("200")}

    transit = build_categorical_variant(links, name="urn:mavedb:test#map3", cross_references=references)

    assert transit is not None
    assert _mappings(transit) == {
        (_CLINGEN, "PA1"): MappingRelation.EXACT_MATCH,
        (_CLINGEN, "CA1"): MappingRelation.RELATED_MATCH,
        (_CLINVAR, "100"): MappingRelation.RELATED_MATCH,
        (_CLINGEN, "CA2"): MappingRelation.RELATED_MATCH,
        (_CLINVAR, "200"): MappingRelation.RELATED_MATCH,
    }


@pytest.mark.unit
def test_exact_match_wins_when_a_related_member_shares_the_identifier():
    """A record reached first through a related member is upgraded when an exact member also carries it."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0),
        _link(level="protein", digest="prot", is_authoritative=False),
        _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0),
    ]
    references = {"prot": _clinvar("100"), "gen": _clinvar("100")}

    transit = build_categorical_variant(links, name="urn:mavedb:test#map4", cross_references=references)

    assert transit is not None
    assert _mappings(transit) == {(_CLINVAR, "100"): MappingRelation.EXACT_MATCH}


@pytest.mark.unit
def test_mappings_are_resolvable_and_absent_without_identifiers():
    """Each coding carries an IRI; a variant with no external identifiers emits no ``mappings`` at all."""
    mapped = build_categorical_variant(
        [_link(level="genomic", digest="gen", is_authoritative=True)],
        name="urn:mavedb:test#map5",
        cross_references={"gen": _gnomad("1-100-A-G")},
    )
    unmapped = build_categorical_variant(
        [_link(level="genomic", digest="gen", is_authoritative=True)], name="urn:mavedb:test#map6"
    )

    assert mapped is not None and mapped.categorical_variant.mappings is not None
    coding = mapped.categorical_variant.mappings[0].coding
    assert [iri.root for iri in coding.iris or []] == [
        "https://gnomad.broadinstitute.org/variant/1-100-A-G?dataset=gnomad_r4"
    ]
    assert unmapped is not None and unmapped.categorical_variant.mappings is None


@pytest.mark.unit
def test_gnomad_mapping_names_its_release():
    """The gnomAD coding carries the release it was matched in; its link is pinned by ``gnomad_variant_url``."""
    transit = build_categorical_variant(
        [_link(level="genomic", digest="gen", is_authoritative=True)],
        name="urn:mavedb:test#map7",
        cross_references={"gen": _gnomad("1-100-A-G", "v2.1.1")},
    )

    assert transit is not None and transit.categorical_variant.mappings is not None
    coding = transit.categorical_variant.mappings[0].coding
    assert coding.systemVersion == "v2.1.1"
    assert [iri.root for iri in coding.iris or []] == [
        "https://gnomad.broadinstitute.org/variant/1-100-A-G?dataset=gnomad_r2_1"
    ]


@pytest.mark.unit
def test_mappings_do_not_depend_on_link_order():
    """The database returns links in no fixed order; the serialized mappings must not follow it."""
    links = [
        _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0, caid="CA1"),
        _link(level="protein", digest="prot", is_authoritative=False, caid="PA1"),
        _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1, caid="CA2"),
    ]
    references = {"cdna": _clinvar("200", "100"), "sibling_gen": _gnomad("1-101-C-T")}

    forward = build_categorical_variant(links, name="urn:mavedb:test#map8", cross_references=references)
    backward = build_categorical_variant(
        list(reversed(links)), name="urn:mavedb:test#map8", cross_references=references
    )

    assert forward is not None and backward is not None
    assert forward.categorical_variant.mappings == backward.categorical_variant.mappings
    assert [(m.coding.system, m.coding.code.root) for m in forward.categorical_variant.mappings or []] == sorted(
        _mappings(forward)
    )


@pytest.mark.unit
def test_member_links_put_the_defining_link_first_and_follow_the_narrow_selection():
    """The prefetch scope matches the built object: defining first, convergent encodings only when wide."""
    defining = _link(level="cdna", digest="cdna", is_authoritative=True, projection_group=0)
    twin = _link(level="genomic", digest="gen", is_authoritative=False, projection_group=0)
    sibling = _link(level="genomic", digest="sibling_gen", is_authoritative=False, projection_group=1)
    links = [twin, sibling, defining]

    assert categorical_member_links(links) == [defining, twin, sibling]
    assert categorical_member_links(links, include_convergent=False) == [defining, twin]
    assert categorical_member_links([twin, sibling]) == []


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
