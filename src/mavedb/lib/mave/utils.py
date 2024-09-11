import re

import pandas as pd

NA_VALUE = "NA"

NULL_VALUES = ("", "na", "nan", "nil", "none", "null", "n/a", "undefined", NA_VALUE)


NULL_VALUES_RE = re.compile("|".join(NULL_VALUES), flags=re.IGNORECASE)
# NULL_VALUES_RE = re.compile(fr'|none|nan|na|undefined|n/a|null|nil|{NA_VALUE}', flags=re.IGNORECASE)


READABLE_NULL_VALUES = [f"'{v}'".format(v) for v in set([v.lower() for v in NULL_VALUES]) if v.strip()] + ["whitespace"]


# html_null_values = [
#     f"<b>{v.strip().lower() or 'whitespace'}</b>" for v in null_values_list
# ]
# HUMANIZED_NULL_VALUES = (
#     f'{", ".join(html_null_values[:-1])} ' f"and " f"{html_null_values[-1]}"
# )


def is_csv_null(value):
    """Return True if a string from a CSV file represents a NULL value."""
    # Avoid any boolean miscasts from comparisons by handling NA types up front.
    if pd.isna(value):
        return True
    # Number 0 is treated as False so that all 0 will be converted to NA value.
    if value == 0:
        return value
    return not value or NULL_VALUES_RE.fullmatch(str(value).strip().lower())
