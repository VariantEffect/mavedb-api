from fastapi import HTTPException
from unittest import TestCase

from mavedb.lib.validation.keywords import validate_keyword, validate_description, validate_duplicates
from mavedb.lib.validation.exceptions import ValidationError

from mavedb.models.controlled_keyword import ControlledKeyword
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation


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
        value = "Other"
        description = "testing"
        validate_description(value, description)

    def test_other_without_extra_description(self):
        # Value is Other, but not provide extra description
        value = "Other"
        description = None
        with self.assertRaises(HTTPException):
            validate_description(value, description)

    # def test_duplicate_keys(self):
    #     controlled_keyword1 = ControlledKeyword(
    #         key="Variant Library Creation Method",
    #         value="Endogenous locus library method",
    #         vocabulary=None,
    #         special=True,
    #         description="Description 1"
    #     )
    #
    #     controlled_keyword2 = ControlledKeyword(
    #         key="Endogenous Locus Library Method",
    #         value="SaCas9",
    #         vocabulary=None,
    #         special=True,
    #         description="Description 2"
    #     )
    #
    #     experiment_controlled_keyword1 = ExperimentControlledKeywordAssociation(
    #         controlled_keyword=controlled_keyword1,
    #         description="Extra description"
    #     )
    #
    #     experiment_controlled_keyword2 = ExperimentControlledKeywordAssociation(
    #         controlled_keyword=controlled_keyword2,
    #         description="Details of delivery method"
    #     )
    #
    #     test_keyword = [
    #         experiment_controlled_keyword1,
    #         experiment_controlled_keyword2
    #     ]
    #
    #     with self.assertRaises(HTTPException):
    #         validate_duplicates(test_keyword)
    #
    def test_duplicate_values(self):
        test_keyword = [
            {
                "keyword": {
                    "key": "Variant Library Creation Method",
                    "value": "Endogenous locus library method",
                    "special": False,
                    "description": "Description 1"
                },
            },
            {
                "keyword": {
                    "key": "Endogenous Locus Library Method System",
                    "value": "Endogenous locus library method",
                    "special": False,
                    "description": "Description 3"
                },
                "description": "Details of delivery method"
            },
        ]
        with self.assertRaises(HTTPException):
            validate_duplicates(test_keyword)
    #
    # def test_duplicate_values_but_they_are_other(self):
    #     test_keyword = [
    #         {
    #             "keyword": {
    #                 "key": "Variant Library Creation Method",
    #                 "value": "Other",
    #                 "special": False,
    #                 "description": "Description 1"
    #             },
    #             "description": "Extra description",
    #         },
    #         {
    #             "keyword": {
    #                 "key": "Delivery method",
    #                 "value": "Other",
    #                 "special": False,
    #                 "description": "Description 2"
    #             },
    #             "description": "Details of delivery method"
    #         },
    #     ]
    #     validate_duplicates(test_keyword)
