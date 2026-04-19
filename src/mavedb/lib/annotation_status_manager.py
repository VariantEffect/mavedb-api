"""Manage annotation statuses for variants.

This module provides functionality to insert and retrieve annotation statuses
for genetic variants, ensuring that only one current status exists per
(variant, annotation type, version) combination.
"""

import logging
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import desc

from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus

logger = logging.getLogger(__name__)

# Default number of pending annotations to accumulate before auto-flushing.
DEFAULT_BATCH_SIZE = 500


class AnnotationStatusManager:
    """
    Manager for handling variant annotation statuses with batched writes.

    Annotations are accumulated in memory and flushed to the database in
    batches (default 500) to reduce round-trips.  Callers **must** call
    :meth:`flush` after the last ``add_annotation`` to persist any remainder.
    """

    def __init__(self, session: Session, *, batch_size: int = DEFAULT_BATCH_SIZE):
        self.session = session
        self.batch_size = batch_size
        self._pending: list[VariantAnnotationStatus] = []
        self._retirement_filters: list[dict] = []

    def add_annotation(
        self,
        variant_id: int,
        annotation_type: AnnotationType,
        status: AnnotationStatus,
        version: Optional[str] = None,
        annotation_data: dict = {},
        current: bool = True,
        replace_all_versions: bool = True,
    ) -> None:
        """
        Stage a new annotation and schedule retirement of previous current rows.

        By default (``replace_all_versions=True``), all existing current annotations for
        (variant, type) are retired regardless of version.

        When ``replace_all_versions=False``, only existing current annotations matching
        (variant, type, version) are retired.

        Writes are accumulated in memory and flushed to the database when
        ``batch_size`` is reached.  Call :meth:`flush` after the last add to
        persist any remaining annotations.

        NOTE:
            This method does not commit the session. The caller is responsible
            for persisting changes (e.g., via ``session.commit()``).
        """
        self._retirement_filters.append(
            {
                "variant_id": variant_id,
                "annotation_type": annotation_type,
                "replace_all_versions": replace_all_versions,
                "version": version,
            }
        )

        self._pending.append(
            VariantAnnotationStatus(
                variant_id=variant_id,
                annotation_type=annotation_type,
                status=status,
                version=version,
                current=current,
                **annotation_data,
            )  # type: ignore[call-arg]
        )

        if len(self._pending) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Flush all pending annotations to the database.

        Retires old ``current=True`` rows in bulk, then inserts all pending
        new rows in a single ``add_all`` + ``flush``.  This replaces the
        previous pattern of 2 flushes per ``add_annotation`` call.
        """
        if not self._pending:
            return

        self._retire_existing()
        self.session.add_all(self._pending)
        self.session.flush()

        logger.debug(f"Flushed {len(self._pending)} annotation statuses")
        self._pending.clear()
        self._retirement_filters.clear()

    def _retire_existing(self) -> None:
        """Bulk-retire existing current annotations for all pending writes.

        Groups retirement filters by (annotation_type, replace_all_versions, version)
        and issues one UPDATE per group, minimizing round-trips.
        """
        # Group filters to minimize UPDATE statements.
        # Key: (annotation_type, replace_all_versions, version) -> list of variant_ids
        groups: dict[tuple, list[int]] = {}
        for f in self._retirement_filters:
            key = (f["annotation_type"], f["replace_all_versions"], f["version"])
            groups.setdefault(key, []).append(f["variant_id"])

        for (annotation_type, replace_all_versions, version), variant_ids in groups.items():
            conditions = [
                VariantAnnotationStatus.variant_id.in_(variant_ids),
                VariantAnnotationStatus.annotation_type == annotation_type,
                VariantAnnotationStatus.current.is_(True),
            ]
            if not replace_all_versions:
                conditions.append(VariantAnnotationStatus.version == version)

            stmt = update(VariantAnnotationStatus).where(*conditions).values(current=False)
            self.session.execute(stmt)

    def get_current_annotation(
        self, variant_id: int, annotation_type: AnnotationType, version: Optional[str] = None
    ) -> Optional[VariantAnnotationStatus]:
        """
        Retrieve the current annotation for a given variant/type/version.

        Flushes pending annotations first to ensure the result is up to date.
        """
        self.flush()

        stmt = select(VariantAnnotationStatus).where(
            VariantAnnotationStatus.variant_id == variant_id,
            VariantAnnotationStatus.annotation_type == annotation_type,
            VariantAnnotationStatus.current.is_(True),
        )

        if version is not None:
            stmt = stmt.where(VariantAnnotationStatus.version == version)

        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def get_annotation_history(
        self,
        variant_id: int,
        annotation_type: AnnotationType,
        version: Optional[str] = None,
    ) -> list[VariantAnnotationStatus]:
        """
        Return the full annotation timeline for a variant/type, newest first.

        Includes both current and retired rows — useful for debugging and
        support investigations.
        """
        self.flush()

        stmt = (
            select(VariantAnnotationStatus)
            .where(
                VariantAnnotationStatus.variant_id == variant_id,
                VariantAnnotationStatus.annotation_type == annotation_type,
            )
            .order_by(desc(VariantAnnotationStatus.id))
        )

        if version is not None:
            stmt = stmt.where(VariantAnnotationStatus.version == version)

        return list(self.session.scalars(stmt).all())

    def get_all_current_annotations(
        self,
        variant_id: int,
    ) -> list[VariantAnnotationStatus]:
        """
        Return all current annotations for a variant, across all types and versions.

        Useful for a quick overview of what annotations are active for a given variant.
        """
        self.flush()

        stmt = (
            select(VariantAnnotationStatus)
            .where(
                VariantAnnotationStatus.variant_id == variant_id,
                VariantAnnotationStatus.current.is_(True),
            )
            .order_by(VariantAnnotationStatus.annotation_type, VariantAnnotationStatus.version)
        )

        return list(self.session.scalars(stmt).all())
