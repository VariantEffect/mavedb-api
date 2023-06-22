from collections import Counter
from typing import Dict

from mavedb.lib.exceptions import ValidationError
from mavedb.lib.mave.constants import (
    VARIANT_SCORE_DATA,
    VARIANT_COUNT_DATA,
    REQUIRED_SCORE_COLUMN,
)


# Only compare the columns name
def validate_columns_match(variant, score_set) -> None:
    """
    Validate that a child matches parents defined columns to keep
    data in sync.
    """
    try:
        if variant.score_columns != score_set.score_columns:
            if Counter(variant.score_columns) != Counter(score_set.score_columns):
                raise ValidationError(
                    f"Variant defines score columns '{variant.score_columns}' "
                    f"but parent defines columns '{score_set.score_columns}. "
                )
        if variant.count_columns != score_set.count_columns:
            # Ignore the column order but comparing the contents. If they have same contents, pass.
            if Counter(variant.count_columns) != Counter(score_set.count_columns):
                raise ValidationError(
                    f"Variant defines count columns '{variant.count_columns}' "
                    f"but parent defines columns '{score_set.count_columns}. "
                )
    except KeyError as error:
        raise ValidationError(f"Missing key {str(error)}")


def validate_variant_json(data: Dict[str, Dict]) -> None:
    """
    Checks a given dictionary to ensure that it is suitable to be used
    as the `data` attribute in a :class:`Variant` instance.

    Parameters
    ----------
    data : dict
        Dictionary of keys mapping to a list.
    """
    expected_keys = [VARIANT_SCORE_DATA, VARIANT_COUNT_DATA]
    for key in expected_keys:
        if key not in data.keys():
            raise ValidationError(
                "Missing the required key '%(key)'.",
                params={"data": data, "key": key},
            )

    if REQUIRED_SCORE_COLUMN not in data[VARIANT_SCORE_DATA]:
        raise ValidationError(
            "Missing required column '%(col)s' in variant's score data.",
            params={"col": REQUIRED_SCORE_COLUMN},
        )
    extras = [k for k in data.keys() if k not in set(expected_keys)]
    if len(extras) > 0:
        extras = [k for k in data.keys() if k not in expected_keys]
        raise ValidationError(
            "Encountered unexpected keys '%(extras)s'.",
            params={"extras": extras},
        )

    # Check the correct data types are given.
    for key in expected_keys:
        if not isinstance(data[key], dict):
            type_ = type(data[key]).__name__
            raise ValidationError(
                "Value for '%(key)' must be a dict not %(type)s.",
                params={"key": key, "type": type_},
            )
