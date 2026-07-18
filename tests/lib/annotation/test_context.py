# ruff: noqa: E402

"""Tests for mavedb.lib.annotation.context — the VA proposition-subject grain (Slice 5.1)."""

import pytest

pytest.importorskip("psycopg2")

from ga4gh.cat_vrs.models import CategoricalVariant
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.context import variant_annotation_context
from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record


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
