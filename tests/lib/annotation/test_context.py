# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.context — the VA proposition-subject grain (Slice 5.1)."""

from datetime import datetime, timezone
from unittest import mock

import pytest

pytest.importorskip("psycopg2")

from ga4gh.cat_vrs.models import CategoricalVariant
from ga4gh.vrs.models import MolecularVariation

from sqlalchemy import select

from mavedb.lib.annotation.context import variant_annotation_context
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record


T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)


def _gnomad_variant(session, db_identifier: str) -> int:
    gnomad_variant = GnomADVariant(
        db_name="gnomAD",
        db_identifier=db_identifier,
        db_version="v4.1",
        allele_count=1,
        allele_number=100,
        allele_frequency=0.01,
    )
    session.add(gnomad_variant)
    session.commit()
    return gnomad_variant.id


def _gnomad_codes(subject) -> list[str]:
    return [
        m.coding.code.root for m in subject.mappings or [] if m.coding.system == "https://gnomad.broadinstitute.org"
    ]


@pytest.mark.integration
class TestVariantAnnotationContextSubject:
    """variant_annotation_context — the VA *proposition* subject grain (Slice 5.1).

    The subject always anchors on the **measured** allele. A protein assay unfurls the full equivalence
    class of encoders; a nucleotide assay keeps its precise coordinate partner + protein consequence and
    excludes the sibling encoders (distinct variants) that the reverse-translation fan left on the record.
    Either way the subject is a Cat-VRS ``CategoricalVariant`` when a projection member exists; it falls
    back to the concrete measured ``MolecularVariation`` when the categorical can't be assembled.
    """

    def test_protein_assay_subject_is_a_categorical_variant(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        seed_mapping_record(
            session,
            mapped_variant.variant,
            assay_level="protein",
            alleles=[
                AlleleSpec(
                    digest="prot", level="protein", is_authoritative=True, post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE
                ),
                AlleleSpec(digest="cdna", level="cdna", post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE),
            ],
        )

        context = variant_annotation_context(session, mapped_variant.variant)

        assert context is not None
        assert isinstance(context.subject_variant, CategoricalVariant)

    def test_nucleotide_assay_subject_is_a_categorical_variant_over_the_measured_change(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """Anchored on the measured nt change: keep its projection_group coordinate partner + the protein
        consequence, exclude the sibling encoder (a distinct variant, different projection group)."""
        mapped_variant = setup_lib_db_with_mapped_variant
        seed_mapping_record(
            session,
            mapped_variant.variant,
            assay_level="cdna",
            alleles=[
                AlleleSpec(
                    digest="cdna",
                    level="cdna",
                    is_authoritative=True,
                    projection_group=0,
                    post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE,
                ),
                # The measured change's precise coordinate partner (same projection group).
                AlleleSpec(
                    digest="gen", level="genomic", projection_group=0, post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE
                ),
                # A sibling encoder of the same protein consequence — distinct variant, other group: excluded.
                AlleleSpec(
                    digest="sibling", level="cdna", projection_group=1, post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE
                ),
                AlleleSpec(digest="prot", level="protein", post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE),
            ],
        )

        context = variant_annotation_context(session, mapped_variant.variant)

        assert context is not None
        assert isinstance(context.subject_variant, CategoricalVariant)
        # defining cdna + gen partner + protein apex; the sibling encoder is dropped.
        assert len(context.subject_variant.members) == 3

    def test_single_allele_subject_falls_back_to_molecular_variation(self, session, setup_lib_db_with_mapped_variant):
        """A lone measured allele (no projection member) → the categorical build yields the concrete allele."""
        mapped_variant = setup_lib_db_with_mapped_variant
        seed_mapping_record(
            session,
            mapped_variant.variant,
            assay_level="cdna",
            alleles=[
                AlleleSpec(
                    digest="cdna", level="cdna", is_authoritative=True, post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE
                ),
            ],
        )

        context = variant_annotation_context(session, mapped_variant.variant)

        assert context is not None
        assert isinstance(context.subject_variant, MolecularVariation)
        assert not isinstance(context.subject_variant, CategoricalVariant)

    def test_unmapped_variant_has_no_context(self, session, setup_lib_db_with_mapped_variant):
        """No live mapping record on the new substrate → no annotation context."""
        context = variant_annotation_context(session, setup_lib_db_with_mapped_variant.variant)

        assert context is None

    def test_subject_mappings_cover_only_the_narrow_members_at_as_of(self, session, setup_lib_db_with_mapped_variant):
        """The subject cross-references its own members only (not the dropped sibling encoder), as of the
        requested instant: a gnomAD match replaced at T1 still maps to the old ID before T1."""
        mapped_variant = setup_lib_db_with_mapped_variant
        old_id = _gnomad_variant(session, "1-100-A-G")
        new_id = _gnomad_variant(session, "1-100-A-T")
        sibling_id = _gnomad_variant(session, "1-101-C-T")
        seed_mapping_record(
            session,
            mapped_variant.variant,
            assay_level="cdna",
            valid_from=T0,
            alleles=[
                AlleleSpec(
                    digest="cdna",
                    level="cdna",
                    is_authoritative=True,
                    projection_group=0,
                    post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE,
                ),
                AlleleSpec(
                    digest="gen",
                    level="genomic",
                    projection_group=0,
                    post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE,
                    gnomad_variant_ids=[old_id],
                ),
                AlleleSpec(
                    digest="sibling",
                    level="cdna",
                    projection_group=1,
                    post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE,
                    gnomad_variant_ids=[sibling_id],
                ),
            ],
        )
        old_link = session.scalar(select(GnomadAlleleLink).where(GnomadAlleleLink.gnomad_variant_id == old_id))
        assert old_link is not None
        old_link.retire(at=T1)
        new_link = GnomadAlleleLink(allele_id=old_link.allele_id, gnomad_variant_id=new_id)
        new_link.valid_from = T1
        session.add(new_link)
        session.commit()

        past = variant_annotation_context(
            session, mapped_variant.variant, as_of=datetime(2020, 6, 1, tzinfo=timezone.utc)
        )
        current = variant_annotation_context(session, mapped_variant.variant)

        assert past is not None and isinstance(past.subject_variant, CategoricalVariant)
        assert current is not None and isinstance(current.subject_variant, CategoricalVariant)
        assert _gnomad_codes(past.subject_variant) == ["1-100-A-G"]
        assert _gnomad_codes(current.subject_variant) == ["1-100-A-T"]

    def test_single_allele_subject_skips_the_cross_reference_fetch(self, session, setup_lib_db_with_mapped_variant):
        """A lone measured allele is served bare, so nothing would carry the cross-references."""
        mapped_variant = setup_lib_db_with_mapped_variant
        seed_mapping_record(
            session,
            mapped_variant.variant,
            assay_level="cdna",
            alleles=[
                AlleleSpec(
                    digest="cdna", level="cdna", is_authoritative=True, post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE
                ),
            ],
        )

        with mock.patch("mavedb.lib.annotation.context.get_allele_cross_references") as fetch:
            context = variant_annotation_context(session, mapped_variant.variant)

        assert context is not None
        fetch.assert_not_called()
