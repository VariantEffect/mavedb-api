from unittest import TestCase

from mavedb.lib.validation.target import validate_sequence_category, validate_target_category, validate_target_sequence
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.target import valid_categories, valid_sequence_types


class TestValidateTargetCategory(TestCase):
    def test_valid(self):
        for category in valid_categories:
            validate_target_category(category)

    def test_invalid_category(self):
        with self.assertRaises(ValidationError):
            validate_target_category("Protein")

    def test_invalid_case(self):
        with self.assertRaises(ValidationError):
            validate_target_category("protein coding")


class TestValidateSequenceCategory(TestCase):
    def test_valid(self):
        for sequence_type in valid_sequence_types:
            validate_sequence_category(sequence_type)

    def test_invalid_category(self):
        with self.assertRaises(ValidationError):
            validate_sequence_category("RNA")

    def test_invalid_case(self):
        with self.assertRaises(ValidationError):
            validate_sequence_category("DNA")


class TestValidateTargetSequence(TestCase):
    def test_valid(self):
        validate_target_sequence("ATGACCAAACAT", "dna")
        validate_target_sequence("STARTREK", "protein")

    def test_infer(self):
        validate_target_sequence("ATGACCAAACAT", "infer")
        validate_target_sequence("STARTREK", "infer")

    def test_type_mismatch(self):
        with self.assertRaises(ValidationError):
            validate_target_sequence("STARTREK", "dna")

    def test_invalid_characters(self):
        with self.assertRaises(ValidationError):
            validate_target_sequence("AUGACCAAACAU", "dna")
        with self.assertRaises(ValidationError):
            validate_target_sequence("AUGACCAAACAU", "infer")
