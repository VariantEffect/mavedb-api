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
