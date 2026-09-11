import pytest

from mavedb.lib.urns import score_set_urn_sort_key, variant_urn_sort_key

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Each of these pairs is one a lexical sort gets wrong. They are the reason the keys exist, so they are
# asserted against plain string ordering too — a test that only checked the key would keep passing if
# someone decided the URNs sort fine on their own.
# ---------------------------------------------------------------------------


class TestScoreSetUrnSortKey:
    def test_unpadded_score_set_number_orders_numerically(self):
        urns = ["urn:mavedb:00000001-a-10", "urn:mavedb:00000001-a-2"]

        assert sorted(urns) == ["urn:mavedb:00000001-a-10", "urn:mavedb:00000001-a-2"]
        assert sorted(urns, key=score_set_urn_sort_key) == [
            "urn:mavedb:00000001-a-2",
            "urn:mavedb:00000001-a-10",
        ]

    def test_experiment_suffix_orders_by_length_then_alphabetically(self):
        """MaveDB assigns experiment suffixes a..z then aa..az, so `z` precedes `aa`."""
        urns = ["urn:mavedb:00000001-aa-1", "urn:mavedb:00000001-b-1", "urn:mavedb:00000001-z-1"]

        assert sorted(urns)[0] == "urn:mavedb:00000001-aa-1"
        assert sorted(urns, key=score_set_urn_sort_key) == [
            "urn:mavedb:00000001-b-1",
            "urn:mavedb:00000001-z-1",
            "urn:mavedb:00000001-aa-1",
        ]

    def test_experiment_sets_order_before_their_experiments(self):
        urns = ["urn:mavedb:00000002-a-1", "urn:mavedb:00000001-z-9"]

        assert sorted(urns, key=score_set_urn_sort_key) == [
            "urn:mavedb:00000001-z-9",
            "urn:mavedb:00000002-a-1",
        ]

    def test_zero_experiment_suffix_is_accepted(self):
        """The experiment URN grammar allows a literal `0` alongside the letter suffixes."""
        assert score_set_urn_sort_key("urn:mavedb:00000001-0-1")[0] == 0

    @pytest.mark.parametrize(
        "urn",
        [
            # An unpublished score set. A SQL cast on the suffix would be handed "467a" and error.
            "tmp:8f14e45f-ceea-467a-9c4f-0b1d2e3f4a5b",
            "not a urn at all",
            "",
            None,
        ],
    )
    def test_undecomposable_urns_sort_after_published_ones_without_raising(self, urn):
        published = "urn:mavedb:00000001-a-1"

        assert sorted([urn, published], key=score_set_urn_sort_key) == [published, urn]

    def test_undecomposable_urns_still_order_deterministically_among_themselves(self):
        urns = ["tmp:b", "tmp:a", "tmp:c"]

        assert sorted(urns, key=score_set_urn_sort_key) == ["tmp:a", "tmp:b", "tmp:c"]


class TestVariantUrnSortKey:
    def test_unpadded_variant_number_orders_numerically(self):
        urns = [f"urn:mavedb:00000001-a-1#{n}" for n in (2, 10, 1)]

        assert sorted(urns)[0] == "urn:mavedb:00000001-a-1#1"
        assert sorted(urns)[1] == "urn:mavedb:00000001-a-1#10"
        assert sorted(urns, key=variant_urn_sort_key) == [
            "urn:mavedb:00000001-a-1#1",
            "urn:mavedb:00000001-a-1#2",
            "urn:mavedb:00000001-a-1#10",
        ]

    def test_variants_group_by_score_set_before_their_number(self):
        urns = ["urn:mavedb:00000001-a-2#1", "urn:mavedb:00000001-a-1#3"]

        assert sorted(urns, key=variant_urn_sort_key) == [
            "urn:mavedb:00000001-a-1#3",
            "urn:mavedb:00000001-a-2#1",
        ]

    def test_unpublished_variant_urn_still_orders_by_its_number(self):
        """A variant of an unpublished score set is `tmp:<uuid>#N`, so the suffix is still parseable."""
        urns = ["tmp:abc#10", "tmp:abc#2"]

        assert sorted(urns, key=variant_urn_sort_key) == ["tmp:abc#2", "tmp:abc#10"]

    @pytest.mark.parametrize("urn", ["urn:mavedb:00000001-a-1", "", None])
    def test_urns_without_a_variant_suffix_sort_last_without_raising(self, urn):
        numbered = "urn:mavedb:00000001-a-1#1"

        assert sorted([urn, numbered], key=variant_urn_sort_key) == [numbered, urn]
