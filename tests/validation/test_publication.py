from unittest import TestCase

from mavedb.lib.validation.publication import (
    validate_publication,
    validate_pubmed,
    validate_biorxiv,
    validate_medrxiv,
    identifier_valid_for,
    validate_db_name,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.publication import valid_dbnames


class TestValidateGenericPublication(TestCase):
    def test_valid_pubmed(self):
        assert validate_publication("20711111") == None

    def test_valid_biorxiv(self):
        assert validate_publication("207222") == None

    def test_valid_medrxiv(self):
        assert validate_publication("20733333") == None

    def test_invalid_identifier(self):
        with self.assertRaises(ValidationError):
            assert validate_publication("2074d44")


# Test each of these sub-validators
class TestValidatePubMedPublication(TestCase):
    def test_valid_pubmed(self):
        assert validate_pubmed("20711111")

    def test_invalid_pubmed(self):
        assert validate_pubmed("invalid_id") == False


class TestValidateBioRxivPublication(TestCase):
    def test_valid_biorxiv_new(self):
        assert validate_biorxiv("2019.12.12.207222")

    def test_valid_biorxiv_old(self):
        assert validate_biorxiv("207222")

    def test_invalid_biorxiv_new(self):
        assert validate_biorxiv("2018.12.12.207222") == False

    def test_invalid_biorxiv_old(self):
        assert validate_biorxiv("20722") == False
        assert validate_biorxiv("2072222") == False
        assert validate_biorxiv("invalid") == False


class TestValidateMedRxivPublication(TestCase):
    def test_valid_medrxiv_new(self):
        assert validate_medrxiv("2019.12.12.20733333")

    def test_valid_medrxiv_old(self):
        assert validate_medrxiv("20733333")

    def test_invalid_medrxiv_new(self):
        assert validate_medrxiv("2018.12.12.20733333") == False

    def test_invalid_medrxiv_old(self):
        assert validate_medrxiv("2073333") == False
        assert validate_medrxiv("207333333") == False
        assert validate_medrxiv("invalid") == False


class TestIdentifierValidFor(TestCase):
    def test_valid_pubmed(self):
        assert identifier_valid_for("20711") == {
            "PubMed": True,
            "bioRxiv": False,
            "medRxiv": False,
        }

    def test_valid_biorxiv(self):
        assert identifier_valid_for("2022.12.12.207222") == {
            "PubMed": False,
            "bioRxiv": True,
            "medRxiv": False,
        }

    def test_valid_medrxiv(self):
        assert identifier_valid_for("2022.12.12.20733333") == {
            "PubMed": False,
            "bioRxiv": False,
            "medRxiv": True,
        }

    def test_valid_pubmed_biorxiv(self):
        assert identifier_valid_for("207222") == {
            "PubMed": True,
            "bioRxiv": True,
            "medRxiv": False,
        }

    def test_valid_pubmed_medrxiv(self):
        assert identifier_valid_for("20733333") == {
            "PubMed": True,
            "bioRxiv": False,
            "medRxiv": True,
        }

    def test_valid_pubmed_none(self):
        assert identifier_valid_for("invalid") == {
            "PubMed": False,
            "bioRxiv": False,
            "medRxiv": False,
        }


class TestValidateDbName(TestCase):
    def test_valid_names(self):
        for name in valid_dbnames:
            assert validate_db_name(name) == None

    def test_empty_name(self):
        with self.assertRaises(ValidationError):
            validate_db_name("   ")

    def test_invalid_name(self):
        with self.assertRaises(ValidationError):
            validate_db_name("invalid db")
