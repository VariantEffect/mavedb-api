"""Append-only writer for the annotation event log.

Buffers :class:`AnnotationEvent` rows and flushes them in batches. This is an
**append-only** log, not a state table: there is no ``current`` flag to
maintain and no retire-on-write step. "Current" is derived at read time
(``DISTINCT ON (subject, annotation_type) … ORDER BY id DESC``) — exposed as
the ``v_current_annotation_events`` view (``mavedb.models.annotation_event_view``).

Each event's *subject* is either a variant or an allele, chosen by
``annotation_type``. The writer validates the subject/type pairing up front so
a mis-subjected event fails with a clear ``ValueError`` rather than a deferred
DB ``CHECK`` violation at flush.
"""

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.sql import desc

from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.annotation_event import ALLELE_SUBJECT_TYPES, VARIANT_SUBJECT_TYPES, AnnotationEvent

logger = logging.getLogger(__name__)

# Default number of pending events to accumulate before auto-flushing.
DEFAULT_BATCH_SIZE = 500


class AnnotationStatusManager:
    """Buffered, append-only writer for :class:`AnnotationEvent` rows.

    Events are accumulated in memory and flushed in batches (default 500) to
    reduce round-trips. Callers **must** call :meth:`flush` after the last
    :meth:`record_event` to persist any remainder.
    """

    def __init__(
        self,
        session: Session,
        job_run_id: Optional[int] = None,
        *,
        score_set_id: Optional[int] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self.session = session
        self.job_run_id = job_run_id
        self.score_set_id = score_set_id
        self.batch_size = batch_size
        self._pending: list[AnnotationEvent] = []

    def record_event(
        self,
        annotation_type: AnnotationType,
        *,
        disposition: Disposition,
        reason: str,
        variant_id: Optional[int] = None,
        allele_id: Optional[int] = None,
        source_version: Optional[str] = None,
        metadata: Optional[dict] = None,
        score_set_id: Optional[int] = None,
    ) -> None:
        """Buffer one terminal observation about a variant or an allele.

        Exactly one of ``variant_id`` / ``allele_id`` must be set, and it must
        match the subject the ``annotation_type`` keys on. ``reason`` is the
        single "what happened" axis across all dispositions (see EventReason).

        Writes are accumulated in memory and flushed when ``batch_size`` is
        reached. Call :meth:`flush` after the last call to persist the
        remainder. Does not commit — the caller owns the transaction.
        """
        self._validate_subject(annotation_type, variant_id, allele_id)

        self._pending.append(
            AnnotationEvent(
                annotation_type=annotation_type,
                variant_id=variant_id,
                allele_id=allele_id,
                disposition=disposition,
                reason=reason,
                source_version=source_version,
                event_metadata=metadata,
                job_run_id=self.job_run_id,
                score_set_id=score_set_id if score_set_id is not None else self.score_set_id,
            )  # type: ignore[call-arg]
        )

        if len(self._pending) >= self.batch_size:
            self.flush()

    @staticmethod
    def _validate_subject(annotation_type: AnnotationType, variant_id: Optional[int], allele_id: Optional[int]) -> None:
        if (variant_id is None) == (allele_id is None):
            raise ValueError("Exactly one of variant_id or allele_id must be set")

        type_value = annotation_type.value if isinstance(annotation_type, AnnotationType) else annotation_type
        if variant_id is not None and type_value not in VARIANT_SUBJECT_TYPES:
            raise ValueError(f"annotation_type {type_value!r} is allele-subject; pass allele_id, not variant_id")
        if allele_id is not None and type_value not in ALLELE_SUBJECT_TYPES:
            raise ValueError(f"annotation_type {type_value!r} is variant-subject; pass variant_id, not allele_id")

    def flush(self) -> None:
        """Insert all pending events in a single ``add_all`` + ``flush``."""
        if not self._pending:
            return

        self.session.add_all(self._pending)
        self.session.flush()

        logger.debug(f"Flushed {len(self._pending)} variant events")
        self._pending.clear()

    def get_current_annotation(
        self,
        annotation_type: AnnotationType,
        *,
        variant_id: Optional[int] = None,
        allele_id: Optional[int] = None,
        source_version: Optional[str] = None,
    ) -> Optional[AnnotationEvent]:
        """Latest event for a single ``(subject, annotation_type)`` key.

        Current status is the newest event by ``id`` — there is no ``current``
        flag to filter on. Flushes pending events first so the result reflects
        buffered writes.
        """
        self.flush()
        self._validate_subject(annotation_type, variant_id, allele_id)

        stmt = select(AnnotationEvent).where(AnnotationEvent.annotation_type == annotation_type)
        if variant_id is not None:
            stmt = stmt.where(AnnotationEvent.variant_id == variant_id)
        else:
            stmt = stmt.where(AnnotationEvent.allele_id == allele_id)
        if source_version is not None:
            stmt = stmt.where(AnnotationEvent.source_version == source_version)

        stmt = stmt.order_by(desc(AnnotationEvent.id)).limit(1)
        return self.session.scalars(stmt).first()

    def get_event_history(
        self,
        annotation_type: AnnotationType,
        *,
        variant_id: Optional[int] = None,
        allele_id: Optional[int] = None,
        source_version: Optional[str] = None,
    ) -> list[AnnotationEvent]:
        """Full event timeline for a ``(subject, annotation_type)`` key, newest first.

        The append-only log retains every observation — skips, reconfirms,
        no-ops — so this is the complete audit trail, not just the current row.
        """
        self.flush()
        self._validate_subject(annotation_type, variant_id, allele_id)

        stmt = select(AnnotationEvent).where(AnnotationEvent.annotation_type == annotation_type)
        if variant_id is not None:
            stmt = stmt.where(AnnotationEvent.variant_id == variant_id)
        else:
            stmt = stmt.where(AnnotationEvent.allele_id == allele_id)
        if source_version is not None:
            stmt = stmt.where(AnnotationEvent.source_version == source_version)

        stmt = stmt.order_by(desc(AnnotationEvent.id))
        return list(self.session.scalars(stmt).all())
