from unittest import TestCase

from mavedb.lib.validation.keywords import (
    validate_keyword,
    validate_description,
    validate_duplicates,
    validate_keyword_keys
)
from mavedb.lib.validation.exceptions import ValidationError

from mavedb.view_models.experiment_controlled_keyword import ExperimentControlledKeywordCreate
from tests.helpers.constants import TEST_DESCRIPTION


class TestKeywordValidators(TestCase):
    """
    Tests that each validator throws the appropriate :class:`ValidationError`
    when passed invalid input.
    """

    def test_ve_invalid_keyword(self):
        with self.assertRaises(ValidationError):
            validate_keyword(555)

    def test_ve_invalid_empty_keyword(self):
        with self.assertRaises(ValidationError):
            validate_keyword("")

    def test_other_with_extra_description(self):
        key = "Key"
        value = "Other"
        description = "testing"
        validate_description(value, key, description)

    def test_other_without_extra_description(self):
        # Value is Other, but not provide extra description
        key = "Key"
        value = "Other"
        description = None
        with self.assertRaises(ValidationError):
            validate_description(value, key, description)

    def test_duplicate_keys(self):
        # Invalid keywords list.
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Variant Library Creation Method",
            "value": "SaCas9",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_duplicates(keyword_list)

    def test_duplicate_values(self):
        # Invalid keywords list.
        keyword1 = {
                    "key": "Variant Library Creation Method",
                    "value": "Endogenous locus library method",
                    "special": False,
                    "description": TEST_DESCRIPTION
                }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
                    "key": "Endogenous Locus Library Method System",
                    "value": "Endogenous locus library method",
                    "special": False,
                    "description": TEST_DESCRIPTION
                }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_duplicates(keyword_list)

    def test_duplicate_values_but_they_are_other(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        validate_duplicates(keyword_list)

    def test_variant_library_value_is_endogenous_and_another_keywords_keys_are_endogenous(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "Endogenous Locus Library Method Mechanism",
            "value": "Nuclease",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_endogenous_but_another_keywords_keys_are_in_vitro(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "value": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "In Vitro Construct Library Method Mechanism",
            "value": "Native locus replacement",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_in_vitro_and_another_keywords_keys_are_both_in_vitro(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "In vitro construct library method",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "value": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "In Vitro Construct Library Method Mechanism",
            "value": "Native locus replacement",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_in_vitro_but_another_keywords_keys_are_endogenous(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "In vitro construct library method",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "Endogenous Locus Library Method Mechanism",
            "value": "Nuclease",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_other(self):
        # Valid keyword
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1]
        validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_other_but_another_keyword_key_is_endogenous(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_value_is_other_but_another_keyword_key_is_in_vitro(self):
        """
        Invalid keyword list.
        If Variant Library Creation Method is Other, none of the rest keywords' keys is endogenous or in vitro method.
        """
        keyword1 = {
            "key": "Variant Library Creation Method",
            "value": "Other",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "value": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)
