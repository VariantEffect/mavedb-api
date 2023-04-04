from functools import partial
from typing import Optional, Union

from mavedb.lib.exceptions import ValidationError

from mavehgvs import Variant, MaveHgvsParseError

from .utils import is_csv_null
from .constants import HGVS_NT_COLUMN, HGVS_PRO_COLUMN, HGVS_SPLICE_COLUMN


def validate_hgvs_string(
    value: Union[str, bytes],
    column: Optional[str] = None,
    splice_present: bool = False,
    targetseq: Optional[str] = None,
    relaxed_ordering: bool = False,
) -> Optional[str]:
    if is_csv_null(value):
        return None

    if hasattr(value, "decode"):
        value = value.decode()
    if not isinstance(value, str):
        raise ValidationError(
            "Variant HGVS values input must be strings. " "'{}' has the type '{}'.".format(value, type(value).__name__)
        )

    if value.lower() == "_sy":
        raise ValidationError("_sy is no longer supported and should be replaced by p.(=)")
    elif value.lower() == "_wt":
        raise ValidationError("_wt is no longer supported and should be replaced by (cgnp).=")

    try:
        variant = Variant(s=value, targetseq=targetseq, relaxed_ordering=relaxed_ordering)
    except MaveHgvsParseError as error:
        raise ValidationError(f"{value}: {str(error)}")

    prefix = variant.prefix.lower()
    if column in ("nt", HGVS_NT_COLUMN):
        if splice_present:
            if prefix not in "g":
                raise ValidationError(
                    f"'{value}' is not a genomic variant (prefix 'g.'). "
                    f"Nucleotide variants must be genomic if transcript "
                    f"variants are also defined."
                )
        else:
            if prefix not in "cn":
                raise ValidationError(
                    f"'{value}' is not a transcript variant. The accepted "
                    f"transcript variant prefixes are 'c.', 'n.'."
                )
    elif column in ("splice", HGVS_SPLICE_COLUMN):
        if prefix not in "cn":
            raise ValidationError(
                f"'{value}' is not a transcript variant. The accepted " f"transcript variant prefixes are 'c.', 'n.'."
            )
    elif column in ("p", HGVS_PRO_COLUMN):
        if prefix not in "p":
            raise ValidationError(
                f"'{value}' is not a protein variant. The accepted " f"protein variant prefix is 'p.'."
            )
    else:
        raise ValueError("Unknown column '{}'. Expected nt, splice or p".format(column))

    return str(variant)


validate_nt_variant = partial(validate_hgvs_string, **{"column": "nt"})
validate_splice_variant = partial(validate_hgvs_string, **{"column": "splice"})
validate_pro_variant = partial(validate_hgvs_string, **{"column": "p"})
