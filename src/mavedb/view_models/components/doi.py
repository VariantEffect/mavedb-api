from typing import Annotated

import idutils
from pydantic import AfterValidator

from mavedb.lib.validation.exceptions import ValidationError


def canonical_doi(value: str) -> str:
    """Validate a DOI and return its canonical form.

    Args:
        value: A DOI, bare or with a ``doi:`` or resolver URL prefix.

    Returns:
        The bare DOI, upper-cased.

    Raises:
        ValidationError: If ``value`` is not a DOI.
    """
    if not idutils.is_doi(value):
        raise ValidationError("'{}' is not a valid DOI identifier.".format(value))

    # DOIs are case-insensitive, and the DOI registry stores them upper-cased. Storing that form keeps a record
    # from citing the same DOI twice.
    return idutils.normalize_doi(value).upper()


Doi = Annotated[str, AfterValidator(canonical_doi)]
"""A DOI field, validated and stored bare and upper-cased."""
