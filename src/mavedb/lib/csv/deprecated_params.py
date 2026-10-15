"""Deprecated query parameters on the score-set CSV endpoints, kept working for backwards compatibility.

``drop_na_columns`` and ``include_post_mapped_hgvs`` were renamed when the CSV export moved to a namespace
vocabulary. FastAPI ignores unknown query parameters, so a client still sending the old names would have
silently received different output rather than an error, and Galaxy calls these endpoints.

Requests using a deprecated name get ``Deprecation`` and ``Warning`` response headers, the parameter is
marked deprecated in OpenAPI, and each use is logged so we can see who is left before removal.

TODO(#XXX): remove this module once clients have migrated.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.deprecation import deprecation_headers, record_deprecated_usage


DROP_NA_COLUMNS_DESCRIPTION = (
    "Deprecated: use `drop_unused_hgvs_columns`, which names what it actually does. This parameter only"
    " ever dropped the HGVS coordinate columns a score set does not use, never every NA column. It will"
    " be removed in a future release; `drop_unused_hgvs_columns` wins if both are given."
)

INCLUDE_POST_MAPPED_HGVS_DESCRIPTION = (
    "Deprecated: request the `mavedb` namespace instead, e.g. `?namespaces=scores&namespaces=mavedb`."
    " Passing true here is equivalent to appending that namespace. It will be removed in a future release."
)

INCLUDE_CUSTOM_COLUMNS_DESCRIPTION = (
    "Deprecated: request the `scores_custom` namespace instead. Passing true here is equivalent to"
    " appending that namespace, whose columns are emitted under the `scores` prefix as before. It will be"
    " removed in a future release."
)


@dataclass
class ResolvedCsvParams:
    """The parameters an endpoint should act on, plus the headers telling the client what it sent."""

    namespaces: List[str]
    drop_unused_hgvs_columns: Optional[bool]
    deprecations: dict[str, str] = field(default_factory=dict)

    def _record(self, name: str, replacement: str) -> None:
        self.deprecations[name] = replacement
        record_deprecated_usage(name, successor=replacement)

    @property
    def response_headers(self) -> dict[str, str]:
        """Headers announcing the deprecation to the client, or nothing at all for a current request."""
        if not self.deprecations:
            return {}

        warnings = "; ".join(
            f"{name} is deprecated, use {replacement}" for name, replacement in sorted(self.deprecations.items())
        )
        return deprecation_headers(successor=None, warning=warnings)


def resolve_deprecated_csv_params(
    *,
    namespaces: Optional[List[str]] = None,
    drop_unused_hgvs_columns: Optional[bool] = None,
    drop_na_columns: Optional[bool] = None,
    include_post_mapped_hgvs: Optional[bool] = None,
    include_custom_columns: Optional[bool] = None,
) -> ResolvedCsvParams:
    """Fold the deprecated spellings into the current ones.

    The current name wins when both are given. The two boolean flags append a namespace rather than
    replacing the requested ones, since both were always additive to whatever columns were asked for.
    """
    resolved = ResolvedCsvParams(
        namespaces=list(namespaces or []),
        drop_unused_hgvs_columns=drop_unused_hgvs_columns,
    )

    if drop_unused_hgvs_columns is None and drop_na_columns is not None:
        resolved.drop_unused_hgvs_columns = drop_na_columns
        resolved._record("drop_na_columns", "drop_unused_hgvs_columns")

    if include_post_mapped_hgvs:
        resolved._record("include_post_mapped_hgvs", "namespaces=mavedb")
        if CsvNamespace.REFERENCE_HGVS not in resolved.namespaces:
            resolved.namespaces.append(CsvNamespace.REFERENCE_HGVS)

    if include_custom_columns:
        resolved._record("include_custom_columns", "namespaces=scores_custom")
        if CsvNamespace.SCORES_CUSTOM not in resolved.namespaces:
            resolved.namespaces.append(CsvNamespace.SCORES_CUSTOM)

    return resolved
