from unittest import TestCase

from src.lib.validation.target import *
from src.lib.validation.exceptions import ValidationError
from src.lib.validation.constants.target import valid_categories, valid_sequence_types


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
            validate_sequence_category("dna")


class TestValidateTargetSequence(TestCase):
    def setUp(self):
        self.target_seq = "ATGACCAAACAT"

    def test_valid(self):
        validate_target_sequence(self.target_seq)

    def test_invalid_characters(self):
        self.target_seq = "AUGACCAAACAU"
        with self.assertRaises(ValidationError):
            validate_target_sequence(self.target_seq)

    def test_invalid_case(self):
        with self.assertRaises(ValidationError):
            validate_target_sequence(self.target_seq.lower())

    def test_invalid_length(self):
        self.target_seq = self.target_seq + "A"
        with self.assertRaises(ValidationError):
            validate_target_sequence(self.target_seq)
