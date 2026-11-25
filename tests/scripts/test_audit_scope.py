# ruff: noqa: E402

from datetime import datetime, timezone

import pytest

pytest.importorskip("asyncclick")
pytest.importorskip("psycopg2")

import asyncclick as click

from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.job_run import JobRun
from mavedb.scripts.audit_scope import resolve_scope


def _score_set(session, make_score_set, urn, translated_at=None, status=JobStatus.SUCCEEDED):
    """A score set with a reverse translation job that finished at ``translated_at`` (None: never ran)."""
    score_set = make_score_set()
    score_set.urn = urn
    session.commit()

    if translated_at is not None:
        session.add(
            JobRun(
                job_type="variant_processing",
                job_function="reverse_translate_variants_for_score_set",
                job_params={"score_set_id": score_set.id},
                status=status,
                finished_at=translated_at,
            )
        )
        session.commit()
    return score_set


OLD = datetime(2026, 9, 15, tzinfo=timezone.utc)
NEW = datetime(2026, 10, 7, tzinfo=timezone.utc)
SINCE = datetime(2026, 10, 1)


@pytest.mark.integration
class TestResolveScope:
    def test_returns_none_when_unscoped(self, session):
        assert resolve_scope(session, (), None, None) is None

    def test_translated_since_keeps_only_score_sets_an_rt_job_finished_processing_since(self, session, make_score_set):
        succeeded = _score_set(session, make_score_set, "urn:mavedb:00000020-a-1", NEW)
        failed = _score_set(session, make_score_set, "urn:mavedb:00000021-a-1", NEW, status=JobStatus.FAILED)
        _score_set(session, make_score_set, "urn:mavedb:00000022-a-1", OLD)
        _score_set(session, make_score_set, "urn:mavedb:00000023-a-1", NEW, status=JobStatus.ERRORED)
        _score_set(session, make_score_set, "urn:mavedb:00000024-a-1")

        assert sorted(resolve_scope(session, (), None, SINCE)) == sorted([succeeded.id, failed.id])

    def test_options_intersect(self, session, make_score_set, sample_user):
        in_collection = _score_set(session, make_score_set, "urn:mavedb:00000025-a-1", NEW)
        stale_in_collection = _score_set(session, make_score_set, "urn:mavedb:00000026-a-1", OLD)
        _score_set(session, make_score_set, "urn:mavedb:00000027-a-1", NEW)

        collection = Collection(name="audit", urn="urn:mavedb:collection-audit", created_by_id=sample_user.id)
        session.add(collection)
        session.flush()
        for position, score_set in enumerate((in_collection, stale_in_collection)):
            session.add(
                CollectionScoreSetAssociation(collection_id=collection.id, score_set_id=score_set.id, position=position)
            )
        session.commit()

        assert sorted(resolve_scope(session, (), collection.urn, None)) == sorted(
            [in_collection.id, stale_in_collection.id]
        )
        assert resolve_scope(session, (), collection.urn, SINCE) == [in_collection.id]

    def test_rejects_an_unknown_collection_and_an_empty_scope(self, session, make_score_set):
        _score_set(session, make_score_set, "urn:mavedb:00000028-a-1", OLD)

        with pytest.raises(click.BadParameter):
            resolve_scope(session, (), "urn:mavedb:collection-missing", None)
        with pytest.raises(click.UsageError):
            resolve_scope(session, (), None, SINCE)
