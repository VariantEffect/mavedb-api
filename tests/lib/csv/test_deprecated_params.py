"""The compatibility layer keeping pre-namespace CSV clients working.

This is the only thing standing between a client that predates the namespace vocabulary and silently
different output: FastAPI ignores unknown query parameters, so an unmapped old spelling would not error,
it would just stop returning columns. Galaxy calls these endpoints, so the translation is pinned here
rather than left to the router tests.
"""

import pytest

from mavedb.lib.csv.deprecated_params import resolve_deprecated_csv_params
from mavedb.lib.csv.namespaces import CsvNamespace


class TestNoDeprecatedParams:
    """A current request must pass through untouched and announce nothing."""

    def test_current_params_are_returned_unchanged(self):
        resolved = resolve_deprecated_csv_params(namespaces=["scores", "gnomad"], drop_unused_hgvs_columns=True)

        assert resolved.namespaces == ["scores", "gnomad"]
        assert resolved.drop_unused_hgvs_columns is True
        assert resolved.deprecations == {}
        assert resolved.response_headers == {}

    def test_no_params_at_all_is_not_an_error(self):
        resolved = resolve_deprecated_csv_params()

        assert resolved.namespaces == []
        assert resolved.drop_unused_hgvs_columns is None
        assert resolved.response_headers == {}

    def test_the_caller_s_namespace_list_is_not_mutated(self):
        """The router hands in its own list; appending to it in place would leak across requests."""
        requested = ["scores"]

        resolve_deprecated_csv_params(namespaces=requested, include_post_mapped_hgvs=True)

        assert requested == ["scores"]


class TestDropNaColumns:
    """``drop_na_columns`` -> ``drop_unused_hgvs_columns``: a rename, so the value carries over as-is."""

    @pytest.mark.parametrize("value", [True, False])
    def test_the_deprecated_value_is_adopted_including_an_explicit_false(self, value):
        """False is a real request to keep the columns, distinct from the parameter being absent."""
        resolved = resolve_deprecated_csv_params(drop_na_columns=value)

        assert resolved.drop_unused_hgvs_columns is value
        assert resolved.deprecations == {"drop_na_columns": "drop_unused_hgvs_columns"}

    @pytest.mark.parametrize("current, deprecated", [(True, False), (False, True)])
    def test_the_current_name_wins_when_both_are_given(self, current, deprecated):
        resolved = resolve_deprecated_csv_params(drop_unused_hgvs_columns=current, drop_na_columns=deprecated)

        assert resolved.drop_unused_hgvs_columns is current

    def test_both_given_announces_nothing(self):
        """Current behaviour: the deprecated value was ignored, so no warning is raised about it."""
        resolved = resolve_deprecated_csv_params(drop_unused_hgvs_columns=True, drop_na_columns=False)

        assert resolved.deprecations == {}
        assert resolved.response_headers == {}


class TestBooleanFlagsBecomeNamespaces:
    """The two flags append a namespace; they were always additive to whatever columns were requested."""

    @pytest.mark.parametrize(
        "flag, expected_namespace, replacement",
        [
            ("include_post_mapped_hgvs", CsvNamespace.REFERENCE_HGVS, "namespaces=mavedb"),
            ("include_custom_columns", CsvNamespace.SCORES_CUSTOM, "namespaces=scores_custom"),
        ],
    )
    def test_a_set_flag_appends_its_namespace_and_keeps_the_requested_ones(self, flag, expected_namespace, replacement):
        resolved = resolve_deprecated_csv_params(namespaces=["scores"], **{flag: True})

        assert resolved.namespaces == ["scores", expected_namespace]
        assert resolved.deprecations == {flag: replacement}

    @pytest.mark.parametrize("flag", ["include_post_mapped_hgvs", "include_custom_columns"])
    @pytest.mark.parametrize("value", [False, None])
    def test_an_unset_flag_is_a_no_op(self, flag, value):
        resolved = resolve_deprecated_csv_params(namespaces=["scores"], **{flag: value})

        assert resolved.namespaces == ["scores"]
        assert resolved.deprecations == {}

    @pytest.mark.parametrize(
        "flag, namespace",
        [
            ("include_post_mapped_hgvs", CsvNamespace.REFERENCE_HGVS),
            ("include_custom_columns", CsvNamespace.SCORES_CUSTOM),
        ],
    )
    def test_a_namespace_already_requested_is_not_appended_twice(self, flag, namespace):
        """A duplicate namespace would emit its columns twice and collide in the header."""
        resolved = resolve_deprecated_csv_params(namespaces=["scores", namespace], **{flag: True})

        assert resolved.namespaces == ["scores", namespace]

    def test_the_appended_tokens_are_the_published_spellings(self):
        """These strings are the request vocabulary; the enum values are frozen for exactly this reason."""
        resolved = resolve_deprecated_csv_params(include_post_mapped_hgvs=True, include_custom_columns=True)

        assert resolved.namespaces == ["mavedb", "scores_custom"]

    def test_every_deprecated_param_can_be_combined(self):
        resolved = resolve_deprecated_csv_params(
            namespaces=["scores"],
            drop_na_columns=True,
            include_post_mapped_hgvs=True,
            include_custom_columns=True,
        )

        assert resolved.namespaces == ["scores", CsvNamespace.REFERENCE_HGVS, CsvNamespace.SCORES_CUSTOM]
        assert resolved.drop_unused_hgvs_columns is True
        assert set(resolved.deprecations) == {"drop_na_columns", "include_post_mapped_hgvs", "include_custom_columns"}


class TestResponseHeaders:
    """RFC 8594 headers are how a client discovers it is on a deprecated path."""

    def test_a_single_deprecation_is_announced(self):
        resolved = resolve_deprecated_csv_params(drop_na_columns=True)

        assert resolved.response_headers == {
            "Deprecation": "true",
            "Warning": '299 - "drop_na_columns is deprecated, use drop_unused_hgvs_columns"',
        }

    def test_several_deprecations_are_joined_in_a_stable_order(self):
        """Sorted, so the header does not churn with parameter order and can be asserted on."""
        resolved = resolve_deprecated_csv_params(include_custom_columns=True, include_post_mapped_hgvs=True)

        assert resolved.response_headers["Warning"] == (
            '299 - "include_custom_columns is deprecated, use namespaces=scores_custom;'
            ' include_post_mapped_hgvs is deprecated, use namespaces=mavedb"'
        )
