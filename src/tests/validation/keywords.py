from unittest import TestCase

from src.lib.validation.keywords import *
from src.lib.validation.exceptions import ValidationError


class TestKeywordValidators(TestCase):
    """
    Tests that each validator throws the appropriate :class:`ValidationError`
    when passed invalid input.
    """

    def test_ve_invalid_keyword(self):
        with self.assertRaises(ValidationError):
            validate_keyword(555)

    def test_ve_invalid_keyword_in_list(self):
        with self.assertRaises(ValidationError):
            validate_keywords(["protein", 555])
