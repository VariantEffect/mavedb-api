from typing import Optional

from pydantic import field_validator, model_validator
from typing_extensions import Self

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class DatasetColumnMetadata(BaseModel):
    """Metadata for individual dataset columns."""

    description: str
    details: Optional[str] = None


class DatasetColumnsBase(BaseModel):
    """Dataset columns view model representing the dataset columns property of a score set."""

    score_columns: Optional[list[str]] = None
    count_columns: Optional[list[str]] = None
    score_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None
    count_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None

    @field_validator("score_columns_metadata", "count_columns_metadata")
    def validate_dataset_columns_metadata(
        cls, v: Optional[dict[str, DatasetColumnMetadata]]
    ) -> Optional[dict[str, DatasetColumnMetadata]]:
        if not v:
            return None
        for val in v.values():
            DatasetColumnMetadata.model_validate(val)
        return v

    @model_validator(mode="after")
    def validate_dataset_columns_metadata_keys(self) -> Self:
        if self.score_columns_metadata is not None and self.score_columns is None:
            raise ValidationError("Score columns metadata cannot be provided without score columns.")
        elif self.score_columns_metadata is not None and self.score_columns is not None:
            for key in self.score_columns_metadata.keys():
                if key not in self.score_columns:
                    raise ValidationError(f"Score column metadata key '{key}' does not exist in score_columns list.")

        if self.count_columns_metadata is not None and self.count_columns is None:
            raise ValidationError("Count columns metadata cannot be provided without count columns.")
        elif self.count_columns_metadata is not None and self.count_columns is not None:
            for key in self.count_columns_metadata.keys():
                if key not in self.count_columns:
                    raise ValidationError(f"Count column metadata key '{key}' does not exist in count_columns list.")
        return self


class SavedDatasetColumns(DatasetColumnsBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)


class DatasetColumns(SavedDatasetColumns):
    pass


class DatasetColumnsCreate(DatasetColumnsBase):
    pass


class DatasetColumnsModify(DatasetColumnsBase):
    pass
