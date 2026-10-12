# ruff: noqa: E402

from datetime import datetime, timezone

import pytest

pytest.importorskip("asyncclick")

import asyncclick as click

from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.collection import Collection
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.variant import Variant
from mavedb.scripts.audit_scope import resolve_scope


def _score_set(session, make_score_set, urn, translated_at):
    """A score set with two variants, each reverse translated at the given instants (None: never)."""
    score_set = make_score_set()
    score_set.urn = urn
    session.commit()

    for i, at in enumerate(translated_at, start=1):
        variant = Variant(urn=f"{urn}#{i}", score_set_id=score_set.id, hgvs_nt=f"c.{i}A>G", data={})
        session.add(variant)
        session.flush()
        if at is not None:
            session.add(
                AnnotationEvent(
                    annotation_type="cross_level_translation",
                    variant_id=variant.id,
                    score_set_id=score_set.id,
                    disposition="present",
                    reason="translated",
                    created_at=at,
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

    def test_translated_since_keeps_only_score_sets_every_variant_of_which_rt_reached(self, session, make_score_set):
        current = _score_set(session, make_score_set, "urn:mavedb:00000020-a-1", [NEW, NEW])
        _score_set(session, make_score_set, "urn:mavedb:00000021-a-1", [OLD, OLD])
        _score_set(session, make_score_set, "urn:mavedb:00000022-a-1", [NEW, OLD])
        _score_set(session, make_score_set, "urn:mavedb:00000023-a-1", [None, None])

        assert resolve_scope(session, (), None, SINCE) == [current.id]

    def test_options_intersect(self, session, make_score_set, sample_user):
        in_collection = _score_set(session, make_score_set, "urn:mavedb:00000024-a-1", [NEW])
        stale_in_collection = _score_set(session, make_score_set, "urn:mavedb:00000025-a-1", [OLD])
        _score_set(session, make_score_set, "urn:mavedb:00000026-a-1", [NEW])

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
        _score_set(session, make_score_set, "urn:mavedb:00000027-a-1", [OLD])

        with pytest.raises(click.BadParameter):
            resolve_scope(session, (), "urn:mavedb:collection-missing", None)
        with pytest.raises(click.UsageError):
            resolve_scope(session, (), None, SINCE)
