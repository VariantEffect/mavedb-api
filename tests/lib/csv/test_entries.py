import pytest

from mavedb.lib.csv.entries import clinvar_namespace_entries
from mavedb.lib.csv.namespaces import CsvNamespaceGroup

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# ClinVar release entries
#
# These go through `clinvar_namespace_entries` rather than asserting how the namespace strings compare,
# because the ordering is what picks the default selection. A test of the comparison alone would keep
# passing if the call site stopped using it.
# ---------------------------------------------------------------------------


class TestClinvarNamespaceEntries:
    def test_entries_are_ordered_newest_release_first(self):
        entries = clinvar_namespace_entries(["clinvar.2024_02", "clinvar.2025_10", "clinvar.2024_11"])

        assert [entry.namespace for entry in entries] == [
            "clinvar.2025_10",
            "clinvar.2024_11",
            "clinvar.2024_02",
        ]

    def test_only_the_newest_release_is_selected_by_default(self):
        entries = clinvar_namespace_entries(["clinvar.2024_02", "clinvar.2025_10", "clinvar.2024_11"])

        assert [entry.selected_by_default for entry in entries] == [True, False, False]

    def test_ordering_survives_uneven_year_widths(self):
        """The decay case: as plain strings "clinvar.999_12" sorts above every four-digit year.

        Ordering by the namespace string would put the malformed release first and hand it the default
        selection, silently changing which ClinVar call a picker opens with.
        """
        entries = clinvar_namespace_entries(["clinvar.999_12", "clinvar.2025_01"])

        assert [entry.namespace for entry in entries] == ["clinvar.2025_01", "clinvar.999_12"]
        assert entries[0].selected_by_default is True
        assert entries[1].selected_by_default is False

    def test_duplicate_releases_are_collapsed(self):
        entries = clinvar_namespace_entries(["clinvar.2025_01", "clinvar.2025_01"])

        assert [entry.namespace for entry in entries] == ["clinvar.2025_01"]

    def test_unlabelable_namespaces_are_dropped_without_taking_the_default(self):
        """An entry that cannot be labelled must not consume the one default slot on its way out."""
        entries = clinvar_namespace_entries(["clinvar.2024_13", "clinvar.2025_01"])

        assert [entry.namespace for entry in entries] == ["clinvar.2025_01"]
        assert entries[0].selected_by_default is True

    def test_entries_are_grouped_as_annotation(self):
        entries = clinvar_namespace_entries(["clinvar.2025_01"])

        assert entries[0].group is CsvNamespaceGroup.ANNOTATION
        assert entries[0].label == "ClinVar significance (January 2025)"

    def test_no_releases_yields_no_entries(self):
        assert clinvar_namespace_entries([]) == []
