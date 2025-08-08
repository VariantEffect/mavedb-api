from unittest import TestCase

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.keywords import (
    validate_code,
    validate_description,
    validate_duplicates,
    validate_keyword,
    validate_keyword_keys,
)
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
        label = "Other"
        description = "testing"
        validate_description(label, key, description)

    def test_other_without_extra_description(self):
        # Value is Other, but not provide extra description
        key = "Key"
        label = "Other"
        description = None
        with self.assertRaises(ValidationError):
            validate_description(label, key, description)

    def test_Gene_Ontology_valid_accession(self):
        key = "Phenotypic Assay Mechanism"
        label = "label"
        code = "GO:1234567"
        validate_code(key, label, code)

    def test_Gene_Ontology_invalid_accession(self):
        key = "Phenotypic Assay Mechanism"
        label = "label"
        code = "GO:123"
        with self.assertRaises(ValidationError):
            validate_code(key, label, code)

    def test_Gene_Ontoloty_term_is_other(self):
        key = "Phenotypic Assay Mechanism"
        label = "Other"
        code = None
        validate_code(key, label, code)

    def test_duplicate_keys(self):
        # Invalid keywords list.
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Variant Library Creation Method",
            "label": "SaCas9",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_duplicates(keyword_list)

    def test_duplicate_labels(self):
        # Invalid keywords list.
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "label": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_duplicates(keyword_list)

    def test_duplicate_labels_but_they_are_other(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2]
        validate_duplicates(keyword_list)

    def test_variant_library_label_is_endogenous_and_another_keywords_keys_are_endogenous(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "Endogenous Locus Library Method Mechanism",
            "label": "Nuclease",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_endogenous_but_another_keywords_keys_are_in_vitro(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Endogenous locus library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "label": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "In Vitro Construct Library Method Mechanism",
            "label": "Native locus replacement",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_in_vitro_and_another_keywords_keys_are_both_in_vitro(self):
        # Valid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "In vitro construct library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "label": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "In Vitro Construct Library Method Mechanism",
            "label": "Native locus replacement",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_in_vitro_but_another_keywords_keys_are_endogenous(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "In vitro construct library method",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword3 = {
            "key": "Endogenous Locus Library Method Mechanism",
            "label": "Nuclease",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj3 = ExperimentControlledKeywordCreate(keyword=keyword3, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1, keyword_obj2, keyword_obj3]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_other(self):
        # Valid keyword
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)
        keyword_list = [keyword_obj1]
        validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_other_but_another_keyword_key_is_endogenous(self):
        # Invalid keyword list
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "Endogenous Locus Library Method System",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)

    def test_variant_library_label_is_other_but_another_keyword_key_is_in_vitro(self):
        """
        Invalid keyword list.
        If Variant Library Creation Method is Other, none of the rest keywords' keys is endogenous or in vitro method.
        """
        keyword1 = {
            "key": "Variant Library Creation Method",
            "label": "Other",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj1 = ExperimentControlledKeywordCreate(keyword=keyword1, description=TEST_DESCRIPTION)

        keyword2 = {
            "key": "In Vitro Construct Library Method System",
            "label": "Oligo-directed mutagenic PCR",
            "special": False,
            "description": TEST_DESCRIPTION,
        }
        keyword_obj2 = ExperimentControlledKeywordCreate(keyword=keyword2, description=TEST_DESCRIPTION)

        keyword_list = [keyword_obj1, keyword_obj2]
        with self.assertRaises(ValidationError):
            validate_keyword_keys(keyword_list)
