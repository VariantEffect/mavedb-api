from unittest import TestCase

from mavedb.lib.validation.urn import (
    validate_mavedb_urn,
    validate_mavedb_urn_experiment,
    validate_mavedb_urn_experimentset,
    validate_mavedb_urn_scoreset,
    validate_mavedb_urn_variant,
    ValidationError,
)


class TestValidateUrn(TestCase):
    def test_valid_mavedb_urn(self):
        validate_mavedb_urn("urn:mavedb:00000002-a-1")

    def test_invalid_mavedb_urn(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn("urn:mavedb:00000002-a-1-z")

    def test_valid_mavedb_urn_experimentset(self):
        validate_mavedb_urn_experimentset("urn:mavedb:00000001")

    def test_invalid_mavedb_urn_experimentset(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_experimentset("")

    def test_valid_mavedb_urn_experiment(self):
        validate_mavedb_urn_experiment("urn:mavedb:00000001-a")

    def test_invalid_mavedb_urn_experiment(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_experiment("")

    def test_valid_mavedb_urn_scoreset(self):
        validate_mavedb_urn_scoreset("urn:mavedb:00000001-a-1")

    def test_invalid_mavedb_urn_scoreset(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_scoreset("")

    def test_valid_mavedb_urn_variant(self):
        # TODO find a valid variant urn
        pass
        # validate_mavedb_urn_variant("")

    def test_invalid_mavedb_urn_variant(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_variant("urn:mavedb:00000002-a-1")  # this is a scoreset urn


class TestValidateTmpUrn(TestCase):
    def test_valid_tmp_mavedb_urn(self):
        validate_mavedb_urn("tmp:0a56b8eb-8e19-4906-8cc7-d17d884330a5")

    def test_invalid_tmp_mavedb_urn(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn("urn:mavedb:00000002-a-1-z")

    def test_valid_tmp_mavedb_urn_experimentset(self):
        validate_mavedb_urn_experimentset("tmp:0a56b8eb-8e19-4906-8cc7-d17d884330a5")

    def test_invalid_tmp_mavedb_urn_experimentset(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_experimentset("")

    def test_valid_tmp_mavedb_urn_experiment(self):
        validate_mavedb_urn_experiment("urn:mavedb:00000001-a")

    def test_invalid_tmp_mavedb_urn_experiment(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_experiment("")

    def test_valid_tmp_mavedb_urn_scoreset(self):
        validate_mavedb_urn_scoreset("tmp:0a56b8eb-8e19-4906-8cc7-d17d884330a5")

    def test_invalid_tmp_mavedb_urn_scoreset(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_scoreset("")

    def test_valid_tmp_mavedb_urn_variant(self):
        validate_mavedb_urn_variant("tmp:0a56b8eb-8e19-4906-8cc7-d17d884330a5")

    def test_invalid_tmp_mavedb_urn_variant(self):
        with self.assertRaises(ValidationError):
            validate_mavedb_urn_variant("urn:mavedb:00000002-a-1")  # this is a scoreset urn
