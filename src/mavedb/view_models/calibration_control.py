"""Pydantic view models for calibration controls.

A calibration control pairs a MaveDB variant with its independently known clinical
significance, serving as empirical ground truth when a calibration's score thresholds
are derived. These models are the API-layer representation used when controls are
created, updated, and served through the calibration endpoints.
"""

from datetime import date
from typing import Any

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.calibration_control_status import CalibrationControlStatus
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.user import SavedUser


class CalibrationControlBase(BaseModel):
    """Fields shared by every calibration control view model.

    ``variant_urn`` is the external identifier for the control's variant; the internal
    primary key is never exposed.
    """

    variant_urn: str
    clinical_status: CalibrationControlStatus


class CalibrationControlModify(CalibrationControlBase):
    """Model used to modify an existing calibration control.

    Carries no additional fields — only the base fields can be updated.
    """

    pass


class CalibrationControlCreate(CalibrationControlModify):
    """Model used to create a calibration control.

    Carries no additional fields — only the modifiable fields are required for creation.
    """

    pass


class SavedCalibrationControl(CalibrationControlBase):
    """Persisted calibration control, including identifier and audit metadata."""

    record_type: str = None  # type: ignore
    _record_type_factory = record_type_validator()(set_record_type)

    id: int
    creation_date: date
    modification_date: date
    created_by: SavedUser
    modified_by: SavedUser

    class Config:
        """Pydantic configuration (ORM mode)."""

        from_attributes = True

    @model_validator(mode="before")
    def generate_variant_urn(cls, data: Any):
        """Expose the control's variant as ``variant_urn`` when building from an ORM object.

        The ORM row references a ``Variant`` relationship, not a bare URN. Mirrors the
        synthetic-field pattern used by ``ScoreCalibrationWithScoreSetUrn``.
        """
        if hasattr(data, "variant"):
            try:
                data.__setattr__("variant_urn", data.variant.urn)
            except (AttributeError, KeyError) as exc:
                raise ValidationError(f"Unable to coerce variant urn for {cls.__name__}: {exc}.")  # type: ignore

        return data


class CalibrationControl(SavedCalibrationControl):
    """Calibration control with its associated calibration and score set URNs."""

    pass
