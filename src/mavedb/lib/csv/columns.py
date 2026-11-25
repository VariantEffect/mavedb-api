"""Planning an export's columns and writing its rows.

Pure functions over rows that are already filled in; nothing here touches the database. What a namespace
*is* lives in ``specs``; this module only applies it.
"""

import csv
import io
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable

from mavedb.lib.csv.specs import CORE_NAMESPACE, CsvRow, RowSource, namespace_spec
from mavedb.lib.mave.utils import NA_VALUE, NULL_VALUES

_OUTPUT_NULL_STRINGS = frozenset(value.lower() for value in NULL_VALUES if value)
"""The null tokens this export recognises, derived from the shared vocabulary rather than restated.

The empty string is dropped because ``_is_output_null`` tests emptiness directly, and ``NA_VALUE`` folds
into ``"na"`` once lowercased.
"""


def _is_output_null(value: Any) -> bool:
    """Whether *value* should be written as the NA sentinel rather than rendered.

    Shares its token vocabulary with ``lib.mave.utils.is_csv_null`` but **not its behaviour**. That one decides
    whether a value read *from* an uploaded file counts as missing, so it copes with pandas NA types and
    treats 0 specially.
    """
    text = str(value).strip().lower()
    return not text or text in _OUTPUT_NULL_STRINGS


def _value_or_na(value: Any, na_rep: str = NA_VALUE) -> str:
    """Return the string representation of *value*, or *na_rep* if the value is null-ish."""
    if _is_output_null(value):
        return na_rep
    return str(value)


@dataclass(frozen=True)
class CsvCell:
    """One column of an export: its header, and how its value is read off a row."""

    header: str
    source: RowSource
    read: Callable[[Any], Any]
    """Called with the row's *source* datum, which may be absent."""


@dataclass(frozen=True)
class CsvPlan:
    """An export's columns, worked out once and applied to every row."""

    cells: tuple[CsvCell, ...]

    parameters: Mapping[RowSource, Mapping[str, str]]
    """For each parameterized source, the requested namespaces and the release or URN each names."""

    needs_score_set: bool
    """Whether the variant query has to eager-load each row's score set."""

    @property
    def headers(self) -> list[str]:
        return [cell.header for cell in self.cells]

    @property
    def sources(self) -> frozenset[RowSource]:
        """The sources a row has to carry for every cell to be read."""
        return frozenset(cell.source for cell in self.cells)

    @classmethod
    def build(cls, dataset_columns: dict, namespaces: Iterable[str], *, namespaced: bool = False) -> "CsvPlan":
        """Plan the core columns plus every requested namespace's, for a score set with *dataset_columns*.

        An unknown namespace contributes no columns rather than being rejected: validating the vocabulary
        belongs to the request layer.
        """
        columns = {}
        for namespace in dict.fromkeys([CORE_NAMESPACE, *namespaces]):
            spec = namespace_spec(namespace)
            columns[namespace] = spec.columns(dataset_columns) if spec else []

        return cls.from_columns(columns, namespaced=namespaced)

    @classmethod
    def from_columns(cls, columns: Mapping[str, Sequence[str]], *, namespaced: bool = False) -> "CsvPlan":
        """Plan exactly *columns*, namespace -> column keys, in order.

        Raises:
            ValueError: if a namespace names a column it can't resolve, or two namespaces resolve to the same
                header. Un-namespaced output strips the prefix that would otherwise keep them apart; the callers
                that ask for it request one namespace each, and this holds them to it rather than letting a
                future caller find out from a broken file.
        """
        cells: list[CsvCell] = []
        parameters: dict[RowSource, dict[str, str]] = {}
        needs_score_set = False

        for namespace, column_keys in columns.items():
            spec = namespace_spec(namespace)
            if spec is None:
                continue

            needs_score_set = needs_score_set or spec.needs_score_set

            if spec.parameter is not None:
                parameter = spec.parameter(namespace)
                if parameter is not None:
                    parameters.setdefault(spec.source, {})[namespace] = parameter
                prefix = namespace
            elif namespace == CORE_NAMESPACE or not namespaced:
                prefix = None
            else:
                prefix = spec.emit_under or namespace

            for column_key in column_keys:
                resolver = spec.resolver(column_key)
                if resolver is None:
                    raise ValueError(f"unrecognized {namespace} column: {column_key}")
                if spec.parameter is not None:
                    resolver = _for_namespace(namespace, resolver)

                header = f"{prefix}.{column_key}" if prefix else column_key
                cells.append(CsvCell(header=header, source=spec.source, read=resolver))

        headers = [cell.header for cell in cells]
        duplicates = sorted({header for header in headers if headers.count(header) > 1})
        if duplicates:
            raise ValueError(
                f"CSV namespaces resolve to duplicate columns: {', '.join(duplicates)}."
                f" Requested namespaces: {', '.join(columns)}."
            )

        return cls(cells=tuple(cells), parameters=parameters, needs_score_set=needs_score_set)

    def without(self, headers: Iterable[str]) -> "CsvPlan":
        """This plan less the columns headed *headers*."""
        dropped = set(headers)
        return CsvPlan(
            cells=tuple(cell for cell in self.cells if cell.header not in dropped),
            parameters=self.parameters,
            needs_score_set=self.needs_score_set,
        )


def _for_namespace(namespace: str, resolver: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Read a parameterized cell from its own namespace's entry in the row's per-namespace datum."""
    return lambda by_namespace: resolver(by_namespace.get(namespace) if by_namespace else None)


def csv_values(plan: CsvPlan, row: CsvRow, na_rep: str = NA_VALUE) -> list[str]:
    """*row*'s cells, in header order, with absent values written as *na_rep*."""
    return [_value_or_na(cell.read(row.get(cell.source)), na_rep) for cell in plan.cells]


def csv_records(plan: CsvPlan, rows: Iterable[CsvRow], na_rep: str = NA_VALUE) -> Iterator[dict[str, str]]:
    """Each of *rows* as a header -> value dict, for callers that build a dataframe rather than a file."""
    headers = plan.headers
    return (dict(zip(headers, csv_values(plan, row, na_rep))) for row in rows)


def render_csv(plan: CsvPlan, batches: Iterable[Sequence[CsvRow]], na_rep: str = NA_VALUE) -> Iterator[str]:
    """CSV text for *batches* of rows, one chunk per batch, with the header leading the first.

    An export with no rows is the header alone.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(plan.headers)

    for rows in batches:
        writer.writerows(csv_values(plan, row, na_rep) for row in rows)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate()

    if buffer.tell():
        yield buffer.getvalue()
