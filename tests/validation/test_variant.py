from unittest import TestCase

from mavedb.lib.validation.variant import validate_hgvs_string
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.constants.general import null_values_list


class TestValidateHgvsString(TestCase):
    def test_passes_on_null(self):
        for v in null_values_list:
            validate_hgvs_string(v)

    def test_error_not_str(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string(1.0)

    def test_error_unknown_column(self):
        with self.assertRaises(ValueError):
            validate_hgvs_string("c.1A>G", column="random")

    def test_error_does_not_match_splice(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("g.G4L", column="splice")

    def test_error_nt_is_not_g_when_splice_present(self):
        validate_hgvs_string("c.1A>G", column="nt", splice_present=False)
        with self.assertRaises(ValidationError):
            validate_hgvs_string("c.1A>G", column="nt", splice_present=True)

    def test_error_does_not_match_nt(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("p.G4L", column="nt")

    def test_error_does_not_match_pro(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("c.1A>G", column="p")

    def test_raises_on_enrich_special_types(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("_wt")
        with self.assertRaises(ValidationError):
            validate_hgvs_string("_sy")

    def test_validates_valid_hgvs(self):
        validate_hgvs_string("c.1A>G", column="nt", splice_present=False)
        validate_hgvs_string("g.1A>G", column="nt", splice_present=True)
        validate_hgvs_string("c.1A>G", column="splice")
        validate_hgvs_string("p.(=)", column="p")


class TestHGVSValidator(TestCase):
    """
    Tests the function :func:`validate_hgvs_string` to see if it is able
    to validate strings which do not comply with the HGVS standard for
    coding, non-coding and nucleotide variants and multi-variants.
    """

    def test_validation_error_not_str_or_bytes(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string([])

    def test_does_not_pass_enrich_wt_hgvs(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("_wt")

    def test_does_not_pass_enrich_sy_hgvs(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("_sy")

    def test_passes_multi(self):
        validate_hgvs_string("p.[Lys4Gly;Lys5Phe]", column="p")
        validate_hgvs_string("c.[1A>G;127_128delinsAGC]", column="nt")
        validate_hgvs_string("c.[1A>G;127_128delinsAGC]", column="splice")

    def test_error_invalid_hgvs(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("c.ad", column="nt")

    def test_error_invalid_nt_prefix(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("r.1a>g", column="nt")

        with self.assertRaises(ValidationError):
            validate_hgvs_string("c.1A>G", column="nt", splice_present=True)

    def test_error_invalid_splice_prefix(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("r.1a>g", column="splice")

    def test_error_invalid_pro_prefix(self):
        with self.assertRaises(ValidationError):
            validate_hgvs_string("r.1a>g", column="p")

    def test_converts_bytes_to_string_before_validation(self):
        validate_hgvs_string(b"c.427A>G", column="splice")

    def test_return_none_for_null(self):
        for c in null_values_list:
            self.assertIsNone(validate_hgvs_string(c, column="nt"))
