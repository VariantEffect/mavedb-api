import enum


class CalibrationControlStatus(str, enum.Enum):
    """Clinical significance of a calibration control variant.

    Deliberately restricted to the two-tier ACMG poles used to anchor a calibration's
    thresholds. Intermediate tiers (VUS, likely pathogenic, likely benign) are excluded
    by design: a control's value as empirical ground truth comes from a confident, binary
    clinical call, not from a graded one.
    """

    pathogenic = "pathogenic"
    benign = "benign"
