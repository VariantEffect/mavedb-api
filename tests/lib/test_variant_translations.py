# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.lib.variant_translations import upsert_variant_translations
from mavedb.models.variant_translation import VariantTranslation


@pytest.mark.unit
class TestUpsertVariantTranslations:
    """Unit tests for upsert_variant_translations.

    Focuses on the INSERT ... ON CONFLICT DO NOTHING semantics: correct
    created/existing counts, deduplication within a batch, and idempotency
    across successive calls within the same transaction.
    """

    def test_inserts_new_pairs(self, session):
        created, existing = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2")])

        assert created == 2
        assert existing == 0
        rows = session.scalars(select(VariantTranslation)).all()
        assert len(rows) == 2

    def test_returns_existing_count_for_committed_rows(self, session):
        session.add(VariantTranslation(aa_clingen_id="PA1", nt_clingen_id="CA1"))
        session.commit()

        created, existing = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2")])

        assert created == 1
        assert existing == 1
        rows = session.scalars(select(VariantTranslation)).all()
        assert len(rows) == 2

    def test_empty_input_returns_zeros(self, session):
        created, existing = upsert_variant_translations(session, [])

        assert created == 0
        assert existing == 0

    def test_deduplicates_duplicate_pairs_within_batch(self, session):
        # Same pair appears twice in the input — should only insert once.
        created, existing = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA1")])

        assert created == 1
        assert existing == 0
        rows = session.scalars(select(VariantTranslation)).all()
        assert len(rows) == 1

    def test_different_nt_under_same_aa_are_distinct_rows(self, session):
        # (PA1, CA1) and (PA1, CA2) share aa_clingen_id but are different rows —
        # ON CONFLICT only fires on an exact composite-key match, so both insert.
        created, existing = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2"), ("PA1", "CA3")])

        assert created == 3
        assert existing == 0

    def test_idempotent_across_calls_without_intermediate_commit(self, session):
        # This is the exact scenario that caused the UniqueViolation crash.
        # Two separate calls within the same transaction share overlapping pairs.
        # The second call must succeed without error (ON CONFLICT DO NOTHING)
        # rather than trying to INSERT a duplicate and blowing up at commit time.
        created1, existing1 = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2")])
        assert created1 == 2
        assert existing1 == 0

        # Overlapping call — CA1 already exists in this transaction, CA3 is new.
        created2, existing2 = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA3")])
        assert created2 == 1
        assert existing2 == 1

        # Commit must succeed — no UniqueViolation.
        session.commit()

        rows = session.scalars(select(VariantTranslation)).all()
        assert len(rows) == 3

    def test_fully_overlapping_second_call_inserts_nothing(self, session):
        upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2")])

        created, existing = upsert_variant_translations(session, [("PA1", "CA1"), ("PA1", "CA2")])
        assert created == 0
        assert existing == 2

        session.commit()
        rows = session.scalars(select(VariantTranslation)).all()
        assert len(rows) == 2


def _draft(digest, *, hgvs_c=None, hgvs_p=None, refget=None, level="cdna"):
    from mavedb.models.allele import Allele

    post_mapped = {"type": "Allele", "location": {"sequenceReference": {"refgetAccession": refget}}} if refget else None
    return Allele(vrs_digest=digest, level=level, hgvs_c=hgvs_c, hgvs_p=hgvs_p, post_mapped=post_mapped)


@pytest.mark.integration
class TestGetOrCreateAllele:
    """One allele per digest, and one allele per HGVS expression."""

    def test_creates_and_flushes_a_new_allele(self, session):
        from mavedb.lib.variant_translations import get_or_create_allele

        allele = get_or_create_allele(session, _draft("digest-a", hgvs_c="NM_000001.1:c.1A>G"))

        assert allele.id is not None

    def test_returns_the_existing_allele_for_the_same_digest(self, session):
        from mavedb.lib.variant_translations import get_or_create_allele

        first = get_or_create_allele(session, _draft("digest-a", hgvs_c="NM_000001.1:c.1A>G"))
        again = get_or_create_allele(session, _draft("digest-a", hgvs_c="NM_000001.1:c.1A>G"))

        assert again is first

    def test_alleles_without_an_hgvs_expression_do_not_conflict(self, session):
        from mavedb.lib.variant_translations import get_or_create_allele

        get_or_create_allele(session, _draft("digest-a"))
        get_or_create_allele(session, _draft("digest-b"))

    def test_same_hgvs_under_a_different_digest_is_a_conflict_that_explains_itself(self, session):
        from mavedb.lib.variant_translations import AlleleIdentityConflictError, get_or_create_allele

        hgvs = "NM_007294.3:c.5448A>T"
        get_or_create_allele(session, _draft("digest-good", hgvs_c=hgvs, refget="SQ.jj1R"))

        with pytest.raises(AlleleIdentityConflictError) as excinfo:
            get_or_create_allele(session, _draft("digest-bad", hgvs_c=hgvs, refget="SQ.bh0R"))

        message = str(excinfo.value)
        for expected in (hgvs, "digest-good", "digest-bad", "SQ.jj1R", "SQ.bh0R", "AlleleIdentityConflictError"):
            assert expected in message

    def test_a_conflict_does_not_add_the_draft_or_poison_the_session(self, session):
        from mavedb.lib.variant_translations import AlleleIdentityConflictError, get_or_create_allele
        from mavedb.models.allele import Allele

        hgvs = "NM_000001.1:c.1A>G"
        get_or_create_allele(session, _draft("digest-good", hgvs_c=hgvs))
        with pytest.raises(AlleleIdentityConflictError):
            get_or_create_allele(session, _draft("digest-bad", hgvs_c=hgvs))

        digests = set(session.scalars(select(Allele.vrs_digest)))
        assert digests == {"digest-good"}
        get_or_create_allele(session, _draft("digest-other", hgvs_c="NM_000001.1:c.2A>G"))

    def test_the_database_index_backstops_a_writer_that_skips_the_check(self, session):
        """A concurrent writer past the application check must still be stopped, by name."""
        from sqlalchemy.exc import IntegrityError

        hgvs = "NM_000001.1:c.1A>G"
        session.add(_draft("digest-a", hgvs_c=hgvs))
        session.flush()
        session.add(_draft("digest-b", hgvs_c=hgvs))

        with pytest.raises(IntegrityError, match="uq_alleles_hgvs"):
            session.flush()
        session.rollback()
