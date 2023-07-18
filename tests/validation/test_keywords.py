from unittest import TestCase

from mavedb.lib.validation.keywords import validate_keyword, validate_keywords
from mavedb.lib.validation.exceptions import ValidationError


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
