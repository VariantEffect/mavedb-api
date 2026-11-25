# ruff: noqa: E402
"""Completeness checks for the CSV exports: each fails when something exists in the code that no test covers.

What needs covering is read from the code (the namespace enum, the source registry, the exports' signatures,
the modules that write CSV), never listed by hand, so it can't fall behind. What covers it is declared here,
and every declaration must point at a snapshot or test that exists. When one of these fails, its message says
what to add.
"""

import ast
import csv
import importlib
import inspect
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Union, get_args, get_origin, get_type_hints

import pytest

pytest.importorskip("psycopg2")

import mavedb
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.score_set import get_score_set_variants_as_csv
from mavedb.lib.csv.sources import SOURCE_LOADERS
from mavedb.lib.csv.specs import RowSource, namespace_spec
from mavedb.lib.csv.variant import get_variant_csv
from tests.lib.csv.test_golden_output import GOLDEN_DIR, SCORE_SET_NAMESPACES, VARIANT_NAMESPACES


@dataclass(frozen=True)
class BySnapshot:
    """A golden snapshot, named as in ``golden/`` without the extension."""

    name: str


@dataclass(frozen=True)
class ByTest:
    """A test, by its pytest node id."""

    node: str


@dataclass(frozen=True)
class Exempt:
    reason: str


Coverage = Union[BySnapshot, ByTest, Exempt]


def _assert_exists(coverage: Coverage) -> None:
    if isinstance(coverage, BySnapshot):
        assert (GOLDEN_DIR / f"{coverage.name}.csv").exists(), f"no snapshot named {coverage.name}"
    elif isinstance(coverage, ByTest):
        path, *names = coverage.node.split("::")
        target = importlib.import_module(path.removesuffix(".py").replace("/", "."))
        for name in names:
            assert hasattr(target, name), f"no test {coverage.node}"
            target = getattr(target, name)
    else:
        assert coverage.reason, "an exemption needs a reason"


def _assert_declared(subject: str, found: set, declared: dict) -> None:
    """*declared* covers exactly *found*: nothing new is undeclared, and nothing declared has since gone."""
    missing = found - declared.keys()
    assert not missing, f"{subject} with no declared coverage: {sorted(map(str, missing))}"
    stale = declared.keys() - found
    assert not stale, f"declared {subject} that no longer exist: {sorted(map(str, stale))}"
    for coverage in declared.values():
        _assert_exists(coverage)


# ---------------------------------------------------------------------------
# Every namespace and source is snapshotted
# ---------------------------------------------------------------------------


def test_snapshots_cover_every_namespace_and_source():
    """A namespace or source the wide cases don't request would ship with no snapshot. Add it to the wide
    lists in ``test_golden_output`` and seed its data in ``tests.helpers.util.csv_export``."""
    snapshotted = {*SCORE_SET_NAMESPACES["wide"], *VARIANT_NAMESPACES["wide"]}

    assert set(CsvNamespace) - snapshotted == set()
    # A parameterized family isn't an enum member, but each brings its own source.
    assert set(RowSource) - {namespace_spec(namespace).source for namespace in snapshotted} == set()


# ---------------------------------------------------------------------------
# Every export is snapshotted
# ---------------------------------------------------------------------------

_CSV_WRITERS = {"render_csv", "csv_records"}
"""Everything that turns rows into CSV goes through one of these, so a module using one is an export."""

_EXPORT_MODULES: dict[str, Coverage] = {
    "mavedb.lib.csv.score_set": BySnapshot("score_set-wide-namespaced-all_hgvs-whole"),
    "mavedb.lib.csv.variant": BySnapshot("variant-mapped-wide"),
    "mavedb.routers.score_sets": ByTest(
        "tests/routers/test_score_set.py::"
        "test_existing_variant_dataframe_is_what_an_upload_of_those_columns_would_have_produced"
    ),
}


def _modules_writing_csv() -> set[str]:
    package = Path(mavedb.__file__).parent
    found = set()
    for path in package.rglob("*.py"):
        module = ".".join(("mavedb", *path.relative_to(package).with_suffix("").parts))
        if module == "mavedb.lib.csv.columns":
            continue
        names = set()
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.alias):
                names.add(node.name)
            elif isinstance(node, ast.Attribute):
                names.add(node.attr)
        if names & _CSV_WRITERS:
            found.add(module)
    return found


def test_every_csv_export_is_covered():
    """A new module writing CSV needs snapshots in ``test_golden_output``, or a test if nobody downloads it,
    declared in ``_EXPORT_MODULES``."""
    _assert_declared("modules writing CSV", _modules_writing_csv(), _EXPORT_MODULES)


# ---------------------------------------------------------------------------
# Every parameter that changes an export's output is exercised
# ---------------------------------------------------------------------------

_EXPORT_PARAMETERS: dict[Callable, dict[str, Coverage]] = {
    get_score_set_variants_as_csv: {
        "namespaced": BySnapshot("score_set-scores-namespaced-all_hgvs-whole"),
        "start": BySnapshot("score_set-scores-bare-all_hgvs-page"),
        "limit": BySnapshot("score_set-scores-bare-all_hgvs-page"),
        "drop_unused_hgvs_columns_flag": BySnapshot("score_set-scores-bare-drop_unused_hgvs-whole"),
        "as_of": BySnapshot("score_set-wide-namespaced-all_hgvs-whole-as_of"),
        "viewer": ByTest(
            "tests/lib/csv/test_variant.py::TestPrivateCalibrationsAreNotDisclosed::"
            "test_a_permitted_caller_receives_it_in_the_score_set_csv"
        ),
    },
    get_variant_csv: {
        "namespaces": BySnapshot("variant-mapped-wide"),
        "may_read_score_set": ByTest(
            "tests/lib/csv/test_variant.py::TestGetVariantCsv::"
            "test_equivalent_measurements_from_unreadable_score_sets_are_left_out"
        ),
        "viewer": ByTest(
            "tests/lib/csv/test_variant.py::TestPrivateCalibrationsAreNotDisclosed::"
            "test_a_permitted_caller_still_receives_it"
        ),
        "na_rep": Exempt("no caller overrides the NA sentinel"),
        "as_of": BySnapshot("variant-mapped-wide-as_of"),
    },
}
"""For each export, where each optional parameter is set to something other than its default."""


def _returns_csv_text(function: Callable) -> bool:
    returns = get_type_hints(function).get("return")
    return returns is str or (get_origin(returns) is Iterator and get_args(returns) == (str,))


def _exports() -> set[Callable]:
    """The public functions returning CSV text in each module that writes CSV for download."""
    exports: set[Callable] = set()
    for module_name, coverage in _EXPORT_MODULES.items():
        if not isinstance(coverage, BySnapshot):
            continue
        module = importlib.import_module(module_name)
        for name, function in inspect.getmembers(module, inspect.isfunction):
            if function.__module__ == module_name and not name.startswith("_") and _returns_csv_text(function):
                exports.add(function)
    return exports


def test_every_export_parameter_is_covered():
    """A new export, or a new optional parameter on one, needs a snapshot or test that sets it, declared in
    ``_EXPORT_PARAMETERS``. Required parameters are set by every case already."""
    exports = {function.__name__ for function in _exports()}
    declared_exports = {function.__name__ for function in _EXPORT_PARAMETERS}
    assert not exports - declared_exports, f"exports with no declared parameters: {sorted(exports - declared_exports)}"
    assert not declared_exports - exports, (
        f"declared exports that no longer exist: {sorted(declared_exports - exports)}"
    )

    for function, declared in _EXPORT_PARAMETERS.items():
        optional = {
            name
            for name, parameter in inspect.signature(function).parameters.items()
            if parameter.default is not inspect.Parameter.empty
        }
        _assert_declared(f"{function.__name__} parameters", optional, declared)


# ---------------------------------------------------------------------------
# Every loaded source says how it answers as_of
# ---------------------------------------------------------------------------

_AS_OF_SNAPSHOT = "score_set-wide-namespaced-all_hgvs-whole-as_of"

_AS_OF_COVERAGE: dict[RowSource, Coverage] = {
    RowSource.MAPPING: ByTest(
        "tests/routers/test_variant.py::TestVariantCsvAsOf::test_csv_echoes_and_reconstructs_at_the_requested_instant"
    ),
    RowSource.GNOMAD: BySnapshot(_AS_OF_SNAPSHOT),
    RowSource.CLINVAR_ENTRY: BySnapshot(_AS_OF_SNAPSHOT),
    RowSource.ANNOTATION: Exempt(
        "calibrations do not yet support as_of, so an as_of export classifies under today's calibrations. TODO(#954)."
    ),
}
"""How each loaded source is shown to reconstruct an earlier instant. A snapshot here must differ from its
current counterpart in that source's columns, so the fixture has to change the source's data over time."""


def _read_snapshot(name: str) -> list[dict[str, str]]:
    with open(GOLDEN_DIR / f"{name}.csv", newline="") as snapshot:
        return list(csv.DictReader(snapshot))


def _source_of(header: str) -> RowSource | None:
    spec = namespace_spec(header.rsplit(".", 1)[0])
    return spec.source if spec else None


def test_every_loaded_source_reconstructs_as_of():
    """A new source needs its loader to honour ``ctx.as_of``, shown by a test or by changing its data in the
    shared fixture after ``before_update``, or an exemption saying why it has no history. Declared in
    ``_AS_OF_COVERAGE``."""
    _assert_declared("loaded sources", set(SOURCE_LOADERS), _AS_OF_COVERAGE)

    for source, coverage in _AS_OF_COVERAGE.items():
        if not isinstance(coverage, BySnapshot):
            continue
        then = _read_snapshot(coverage.name)
        now = _read_snapshot(coverage.name.removesuffix("-as_of"))
        headers = [header for header in now[0] if _source_of(header) is source]

        assert headers, f"{coverage.name} has no {source} columns"
        assert any(row_then[header] != row_now[header] for row_then, row_now in zip(then, now) for header in headers), (
            f"{source}'s data doesn't change between {coverage.name} and now, so it can't show as_of is honoured"
        )
