"""Manage annotation statuses for variants.

This module provides functionality to insert and retrieve annotation statuses
for genetic variants, ensuring that only one current status exists per
(variant, annotation type, version) combination.
"""

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.variant_annotation_status import VariantAnnotationStatus

logger = logging.getLogger(__name__)


class AnnotationStatusManager:
    """
    Manager for handling variant annotation statuses.

    Attributes:
        session (Session): The SQLAlchemy session used for database operations.

    Methods:
        add_annotation(
            variant_id: int,
            annotation_type: AnnotationType,
            version: Optional[str],
            annotation_data: dict,
            current: bool = True
        ) -> VariantAnnotationStatus:
            Inserts a new annotation status and marks previous ones as not current.

        get_current_annotation(
            variant_id: int,
            annotation_type: AnnotationType,
            version: Optional[str] = None
        ) -> Optional[VariantAnnotationStatus]:
            Retrieves the current annotation status for a given variant/type/version.
    """

    def __init__(self, session: Session):
        self.session = session

    def add_annotation(
        self,
        variant_id: int,
        annotation_type: AnnotationType,
        status: AnnotationStatus,
        version: Optional[str] = None,
        annotation_data: dict = {},
        current: bool = True,
        replace_all_versions: bool = True,
    ) -> VariantAnnotationStatus:
        """
        Insert a new annotation and mark previous ones as not current.

        By default (``replace_all_versions=True``), all existing current annotations for
        (variant, type) are retired regardless of version. This is appropriate for
        pipelines like VRS mapping where a new run fully supersedes all
        previous results across every version.

        When ``replace_all_versions=False``, only existing current annotations matching
        (variant, type, version) are retired. Use this for pipelines where a new run
        should only supersede results of the same version.

        Args:
            variant_id (int): The ID of the variant being annotated.
            annotation_type (AnnotationType): The type of annotation (e.g., 'vrs', 'clinvar').
            version (Optional[str]): The version of the annotation source.
            annotation_data (dict): Additional data for the annotation status.
            current (bool): Whether this annotation is the current one.
            replace_all_versions (bool): When True, retire all current annotations for
                (variant, type) regardless of version. When False (default), only
                retire those matching (variant, type, version).

        Returns:
            VariantAnnotationStatus: The newly created annotation status record.

        Side Effects:
            - Updates existing records to set current=False.
            - Adds a new VariantAnnotationStatus record to the database session.

        NOTE:
            - This method does not commit the session and only flushes to the database. The caller
              is responsible for persisting any changes (e.g., by calling session.commit()).
        """
        logger.debug(
            f"Adding annotation for variant_id={variant_id}, annotation_type={annotation_type}, version={version}"
        )

        # Find existing current annotations to be replaced.
        # With replace_all_versions=True, retire all versions; otherwise only the matching version.
        retirement_filter = [
            VariantAnnotationStatus.variant_id == variant_id,
            VariantAnnotationStatus.annotation_type == annotation_type,
            VariantAnnotationStatus.current.is_(True),
        ]
        if not replace_all_versions:
            retirement_filter.append(VariantAnnotationStatus.version == version)

        existing_current = (
            self.session.execute(select(VariantAnnotationStatus).where(*retirement_filter)).scalars().all()
        )

        for var_ann in existing_current:
            logger.debug(
                f"Replacing current annotation {var_ann.id} for variant_id={variant_id}, annotation_type={annotation_type}, version={version}"
            )
            var_ann.current = False

        self.session.flush()

        new_status = VariantAnnotationStatus(
            variant_id=variant_id,
            annotation_type=annotation_type,
            status=status,
            version=version,
            current=current,
            **annotation_data,
        )  # type: ignore[call-arg]

        self.session.add(new_status)
        self.session.flush()

        logger.info(
            f"Successfully added annotation for variant_id={variant_id}, annotation_type={annotation_type}, version={version}"
        )
        return new_status

    def get_current_annotation(
        self, variant_id: int, annotation_type: AnnotationType, version: Optional[str] = None
    ) -> Optional[VariantAnnotationStatus]:
        """
        Retrieve the current annotation for a given variant/type/version.

        Args:
            variant_id (int): The ID of the variant.
            annotation_type (AnnotationType): The type of annotation.
            version (Optional[str]): The version of the annotation source.

        Returns:
            Optional[VariantAnnotationStatus]: The current annotation status record, or None if not found.
        """
        stmt = select(VariantAnnotationStatus).where(
            VariantAnnotationStatus.variant_id == variant_id,
            VariantAnnotationStatus.annotation_type == annotation_type,
            VariantAnnotationStatus.current.is_(True),
        )

        if version is not None:
            stmt = stmt.where(VariantAnnotationStatus.version == version)

        result = self.session.execute(stmt)
        return result.scalar_one_or_none()
