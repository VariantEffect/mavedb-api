# ruff: noqa: E402

from unittest.mock import Mock, patch

import pytest

pytest.importorskip("psycopg2")

import pandas as pd

from mavedb.lib.validation.constants.general import calibration_class_column_name, calibration_variant_column_name
from mavedb.lib.validation.dataframe.calibration import (
    validate_and_standardize_calibration_classes_dataframe,
    validate_calibration_classes,
    validate_calibration_df_column_names,
    validate_calibration_variant_urns,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import score_calibration


class TestValidateAndStandardizeCalibrationClassesDataframe:
    """Test suite for validate_and_standardize_calibration_classes_dataframe function."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies for the function."""
        with (
            patch("mavedb.lib.validation.dataframe.calibration.standardize_dataframe") as mock_standardize,
            patch("mavedb.lib.validation.dataframe.calibration.validate_no_null_rows") as mock_validate_no_null,
            patch("mavedb.lib.validation.dataframe.calibration.validate_variant_column") as mock_validate_variant,
            patch("mavedb.lib.validation.dataframe.calibration.validate_data_column") as mock_validate_data,
        ):
            yield {
                "standardize_dataframe": mock_standardize,
                "validate_no_null_rows": mock_validate_no_null,
                "validate_variant_column": mock_validate_variant,
                "validate_data_column": mock_validate_data,
            }

    def test_validate_and_standardize_calibration_classes_dataframe_success(self, mock_dependencies):
        """Test successful validation and standardization."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123

        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame(
            {
                calibration_variant_column_name.upper(): ["var1", "var2"],
                calibration_class_column_name.upper(): ["A", "B"],
            }
        )
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1", "var2"], calibration_class_column_name: ["A", "B"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        mock_scalars = Mock()
        mock_scalars.all.return_value = ["var1", "var2"]
        mock_db.scalars.return_value = mock_scalars

        mock_classification1 = Mock()
        mock_classification1.class_ = "A"
        mock_classification2 = Mock()
        mock_classification2.class_ = "B"
        mock_calibration.functional_classifications = [mock_classification1, mock_classification2]

        result = validate_and_standardize_calibration_classes_dataframe(
            mock_db, mock_score_set, mock_calibration, input_df
        )

        assert result.equals(standardized_df)
        mock_dependencies["standardize_dataframe"].assert_called_once()
        mock_dependencies["validate_no_null_rows"].assert_called_once_with(standardized_df)
        mock_dependencies["validate_variant_column"].assert_called_once()
        mock_dependencies["validate_data_column"].assert_called_once()

    def test_validate_and_standardize_calibration_classes_dataframe_not_class_based(self):
        """Test ValidationError when calibration is not class-based."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = False
        input_df = pd.DataFrame({"variant": ["var1"], "class": ["A"]})

        with pytest.raises(
            ValidationError,
            match="Calibration classes file can only be provided for functional classification calibrations.",
        ):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_invalid_column_names(self, mock_dependencies):
        """Test ValidationError when column validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], "invalid": ["A"]})
        standardized_df = pd.DataFrame({calibration_variant_column_name: ["var1"], "invalid": ["A"]})

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_class_column_name}'"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_null_rows(self, mock_dependencies):
        """Test ValidationError when null rows validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df
        mock_dependencies["validate_no_null_rows"].side_effect = ValidationError("null rows detected")

        with pytest.raises(ValidationError, match="null rows detected"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_invalid_variants(self, mock_dependencies):
        """Test ValidationError when variant URN validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        mock_scalars = Mock()
        mock_scalars.all.return_value = []
        mock_db.scalars.return_value = mock_scalars

        with pytest.raises(ValidationError, match="The following variant URNs do not exist in the score set: var1"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_invalid_classes(self, mock_dependencies):
        """Test ValidationError when class validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        mock_scalars = Mock()
        mock_scalars.all.return_value = ["var1"]
        mock_db.scalars.return_value = mock_scalars

        mock_calibration.functional_classifications = None

        with pytest.raises(
            ValidationError, match="Calibration must have functional classifications defined for class validation."
        ):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_unexpected_column(self, mock_dependencies):
        """Test ValidationError when unexpected column is present."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame(
            {
                calibration_variant_column_name: ["var1"],
                calibration_class_column_name: ["A"],
                "extra1": ["X"],
                "extra2": ["Y"],
            }
        )
        standardized_df = pd.DataFrame(
            {
                calibration_variant_column_name: ["var1"],
                calibration_class_column_name: ["A"],
                "extra1": ["X"],
                "extra2": ["Y"],
            }
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        with pytest.raises(ValidationError, match="unexpected column\(s\) in calibration classes file: extra1, extra2"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_variant_column_validation_fails(
        self, mock_dependencies
    ):
        """Test ValidationError when variant column validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df
        mock_dependencies["validate_variant_column"].side_effect = ValidationError("invalid variant column")

        with pytest.raises(ValidationError, match="invalid variant column"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_data_column_validation_fails(
        self, mock_dependencies
    ):
        """Test ValidationError when data column validation fails."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df
        mock_dependencies["validate_data_column"].side_effect = ValidationError("invalid data column")

        mock_scalars = Mock()
        mock_scalars.all.return_value = ["var1"]
        mock_db.scalars.return_value = mock_scalars

        with pytest.raises(ValidationError, match="invalid data column"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)

    def test_validate_and_standardize_calibration_classes_dataframe_mixed_case_columns(self, mock_dependencies):
        """Test successful validation with mixed case column names."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name.upper(): ["A"]}
        )
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        mock_scalars = Mock()
        mock_scalars.all.return_value = ["var1"]
        mock_db.scalars.return_value = mock_scalars

        mock_classification = Mock()
        mock_classification.class_ = "A"
        mock_calibration.functional_classifications = [mock_classification]

        result = validate_and_standardize_calibration_classes_dataframe(
            mock_db, mock_score_set, mock_calibration, input_df
        )

        assert result.equals(standardized_df)
        mock_dependencies["validate_data_column"].assert_called_once_with(
            standardized_df[calibration_class_column_name], force_numeric=False
        )

    def test_validate_and_standardize_calibration_classes_dataframe_with_score_calibration_modify(
        self, mock_dependencies
    ):
        """Test function works with ScoreCalibrationModify object."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_score_set.id = 123
        mock_calibration = Mock(spec=score_calibration.ScoreCalibrationModify)
        mock_calibration.class_based = True

        input_df = pd.DataFrame({calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]})
        standardized_df = pd.DataFrame(
            {calibration_variant_column_name: ["var1"], calibration_class_column_name: ["A"]}
        )

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        mock_scalars = Mock()
        mock_scalars.all.return_value = ["var1"]
        mock_db.scalars.return_value = mock_scalars

        mock_classification = Mock()
        mock_classification.class_ = "A"
        mock_calibration.functional_classifications = [mock_classification]

        result = validate_and_standardize_calibration_classes_dataframe(
            mock_db, mock_score_set, mock_calibration, input_df
        )

        assert result.equals(standardized_df)

    def test_validate_and_standardize_calibration_classes_dataframe_empty_dataframe(self, mock_dependencies):
        """Test ValidationError with empty dataframe."""
        mock_db = Mock()
        mock_score_set = Mock()
        mock_calibration = Mock()
        mock_calibration.class_based = True

        input_df = pd.DataFrame()
        standardized_df = pd.DataFrame()

        mock_dependencies["standardize_dataframe"].return_value = standardized_df

        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_variant_column_name}'"):
            validate_and_standardize_calibration_classes_dataframe(mock_db, mock_score_set, mock_calibration, input_df)


class TestValidateCalibrationDfColumnNames:
    """Test suite for validate_calibration_df_column_names function."""

    def test_validate_calibration_df_column_names_success(self):
        """Test successful validation with correct column names."""
        df = pd.DataFrame(
            {calibration_variant_column_name: ["var1", "var2"], calibration_class_column_name: ["A", "B"]}
        )

        validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_case_insensitive(self):
        """Test successful validation with different case column names."""
        df = pd.DataFrame(
            {
                calibration_variant_column_name.upper(): ["var1", "var2"],
                calibration_class_column_name.upper(): ["A", "B"],
            }
        )

        validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_mixed_case(self):
        """Test successful validation with mixed case column names."""
        df = pd.DataFrame(
            {
                calibration_variant_column_name.capitalize(): ["var1", "var2"],
                calibration_class_column_name.capitalize(): ["A", "B"],
            }
        )

        validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_non_string_columns(self):
        """Test ValidationError when column names are not strings."""
        df = pd.DataFrame({123: ["var1", "var2"], calibration_class_column_name: ["A", "B"]})

        # Act & Assert
        with pytest.raises(ValidationError, match="column names must be strings"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_empty_column_name(self):
        """Test ValidationError when column names are empty."""
        df = pd.DataFrame(columns=["", calibration_variant_column_name])

        # Act & Assert
        with pytest.raises(ValidationError, match="column names cannot be empty or whitespace"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_whitespace_column_name(self):
        """Test ValidationError when column names contain only whitespace."""
        df = pd.DataFrame(columns=["   ", calibration_class_column_name])

        # Act & Assert
        with pytest.raises(ValidationError, match="column names cannot be empty or whitespace"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_tab_whitespace(self):
        """Test ValidationError when column names contain only tab characters."""
        df = pd.DataFrame(columns=["\t\t", calibration_class_column_name])

        # Act & Assert
        with pytest.raises(ValidationError, match="column names cannot be empty or whitespace"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_missing_variant_column(self):
        """Test ValidationError when variant column is missing."""
        df = pd.DataFrame({calibration_class_column_name: ["A", "B"], "other": ["X", "Y"]})

        # Act & Assert
        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_variant_column_name}'"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_missing_class_column(self):
        """Test ValidationError when class column is missing."""
        df = pd.DataFrame({calibration_variant_column_name: ["var1", "var2"], "other": ["X", "Y"]})

        # Act & Assert
        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_class_column_name}'"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_missing_both_required_columns(self):
        """Test ValidationError when both required columns are missing."""
        df = pd.DataFrame({"other1": ["X", "Y"], "other2": ["A", "B"]})

        # Act & Assert
        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_variant_column_name}'"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_unexpected_extra_columns(self):
        """Test ValidationError when unexpected columns are present."""
        df = pd.DataFrame(
            {
                calibration_variant_column_name: ["var1", "var2"],
                calibration_class_column_name: ["A", "B"],
                "extra_column": ["X", "Y"],
            }
        )

        # Act & Assert
        with pytest.raises(ValidationError, match="unexpected column\(s\) in calibration classes file: extra_column"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_fewer_than_expected_columns(self):
        """Test ValidationError when fewer columns than expected are present."""
        df = pd.DataFrame({calibration_variant_column_name: ["var1", "var2"]})

        # Act & Assert
        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_class_column_name}'"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_duplicate_columns_different_case(self):
        """Test ValidationError when duplicate columns exist with different cases."""
        df = pd.DataFrame(
            columns=[
                calibration_variant_column_name,
                calibration_variant_column_name.upper(),
                calibration_class_column_name,
            ]
        )

        # Act & Assert
        with pytest.raises(ValidationError, match="duplicate column names are not allowed \(case-insensitive\)"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_empty_dataframe(self):
        """Test ValidationError when dataframe has no columns."""
        df = pd.DataFrame()

        # Act & Assert
        with pytest.raises(ValidationError, match=f"missing required column: '{calibration_variant_column_name}'"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_with_numeric_and_string_mix(self):
        """Test ValidationError when columns mix numeric and string types."""
        df = pd.DataFrame(columns=["variant", 42.5])

        # Act & Assert
        with pytest.raises(ValidationError, match="column names must be strings"):
            validate_calibration_df_column_names(df)

    def test_validate_calibration_df_column_names_newline_in_whitespace(self):
        """Test ValidationError when column names contain newline characters."""
        df = pd.DataFrame(columns=["\n\n", "class"])

        # Act & Assert
        with pytest.raises(ValidationError, match="column names cannot be empty or whitespace"):
            validate_calibration_df_column_names(df)


class TestValidateCalibrationVariantUrns:
    """Test suite for validate_calibration_variant_urns function."""

    def test_validate_calibration_variant_urns_success(self):
        """Test successful validation when all variant URNs exist in score set."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:1", "urn:variant:2", "urn:variant:3"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 123

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2", "urn:variant:3"])

        validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

        mock_db.scalars.assert_called_once()

    def test_validate_calibration_variant_urns_missing_variants(self):
        """Test ValidationError when some variant URNs don't exist in score set."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:1", "urn:variant:2"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 123

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2", "urn:variant:3"])

        # Act & Assert
        with pytest.raises(
            ValidationError, match="The following variant URNs do not exist in the score set: urn:variant:3"
        ):
            validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_multiple_missing_variants(self):
        """Test ValidationError when multiple variant URNs don't exist in score set."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:1"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 456

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2", "urn:variant:3"])

        # Act & Assert
        with pytest.raises(
            ValidationError,
            match="The following variant URNs do not exist in the score set: urn:variant:2, urn:variant:3",
        ):
            validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_all_missing(self):
        """Test ValidationError when all variant URNs are missing from score set."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = []
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 789

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2"])

        # Act & Assert
        with pytest.raises(
            ValidationError,
            match="The following variant URNs do not exist in the score set: urn:variant:1, urn:variant:2",
        ):
            validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_empty_series(self):
        """Test successful validation with empty variant URNs series."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = []
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 123

        variant_urns = pd.Series([], dtype=object)

        # Act & Assert - should not raise any exception
        validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_single_variant(self):
        """Test successful validation with single variant URN."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:single"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 123

        variant_urns = pd.Series(["urn:variant:single"])

        # Act & Assert - should not raise any exception
        validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_duplicate_urns_in_series(self):
        """Test validation with duplicate URNs in input series."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:1", "urn:variant:2"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 123

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2", "urn:variant:1", "urn:variant:2"])

        # Act & Assert - should not raise any exception
        validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

    def test_validate_calibration_variant_urns_database_query_parameters(self):
        """Test that database query is constructed with correct parameters."""
        mock_db = Mock()
        mock_scalars = Mock()
        mock_scalars.all.return_value = ["urn:variant:1", "urn:variant:2"]
        mock_db.scalars.return_value = mock_scalars

        mock_score_set = Mock()
        mock_score_set.id = 999

        variant_urns = pd.Series(["urn:variant:1", "urn:variant:2"])

        validate_calibration_variant_urns(mock_db, mock_score_set, variant_urns)

        mock_db.scalars.assert_called_once()


class TestValidateCalibrationClasses:
    """Test suite for validate_calibration_classes function."""

    def test_validate_calibration_classes_success(self):
        """Test successful validation when all classes match."""
        mock_classification1 = Mock()
        mock_classification1.class_ = "class_a"
        mock_classification2 = Mock()
        mock_classification2.class_ = "class_b"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification1, mock_classification2]

        classes = pd.Series(["class_a", "class_b", "class_a"])

        validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_no_functional_classifications(self):
        """Test ValidationError when calibration has no functional classifications."""
        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = None
        classes = pd.Series(["class_a", "class_b"])

        with pytest.raises(
            ValidationError, match="Calibration must have functional classifications defined for class validation."
        ):
            validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_empty_functional_classifications(self):
        """Test ValidationError when calibration has empty functional classifications."""
        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = []
        classes = pd.Series(["class_a", "class_b"])

        with pytest.raises(
            ValidationError, match="Calibration must have functional classifications defined for class validation."
        ):
            validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_undefined_classes_in_series(self):
        """Test ValidationError when series contains undefined classes."""
        mock_classification = Mock()
        mock_classification.class_ = "class_a"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification]

        classes = pd.Series(["class_a", "class_b", "class_c"])

        with pytest.raises(
            ValidationError, match="The following classes are not defined in the calibration: class_b, class_c"
        ):
            validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_missing_defined_classes(self):
        """Test ValidationError when defined classes are missing from series."""
        mock_classification1 = Mock()
        mock_classification1.class_ = "class_a"
        mock_classification2 = Mock()
        mock_classification2.class_ = "class_b"
        mock_classification3 = Mock()
        mock_classification3.class_ = "class_c"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification1, mock_classification2, mock_classification3]

        classes = pd.Series(["class_a", "class_b"])

        with pytest.raises(
            ValidationError, match="Some defined classes in the calibration are missing from the classes file."
        ):
            validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_with_modify_object(self):
        """Test function works with ScoreCalibrationModify object."""
        mock_classification = Mock()
        mock_classification.class_ = "class_a"

        calibration = Mock(spec=score_calibration.ScoreCalibrationModify)
        calibration.functional_classifications = [mock_classification]

        classes = pd.Series(["class_a"])

        validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_empty_series(self):
        """Test ValidationError when classes series is empty but calibration has classifications."""
        mock_classification = Mock()
        mock_classification.class_ = "class_a"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification]

        classes = pd.Series([], dtype=object)

        with pytest.raises(
            ValidationError, match="Some defined classes in the calibration are missing from the classes file."
        ):
            validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_duplicate_classes_in_series(self):
        """Test successful validation with duplicate classes in series."""
        mock_classification1 = Mock()
        mock_classification1.class_ = "class_a"
        mock_classification2 = Mock()
        mock_classification2.class_ = "class_b"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification1, mock_classification2]

        classes = pd.Series(["class_a", "class_a", "class_b", "class_b", "class_a"])

        validate_calibration_classes(calibration, classes)

    def test_validate_calibration_classes_single_class(self):
        """Test successful validation with single class."""
        mock_classification = Mock()
        mock_classification.class_ = "single_class"

        calibration = Mock(spec=score_calibration.ScoreCalibrationCreate)
        calibration.functional_classifications = [mock_classification]

        classes = pd.Series(["single_class", "single_class"])

        validate_calibration_classes(calibration, classes)
