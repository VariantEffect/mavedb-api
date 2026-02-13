import enum


class ScoreCalibrationRelation(str, enum.Enum):
    threshold = "threshold"
    classification = "classification"
    method = "method"
