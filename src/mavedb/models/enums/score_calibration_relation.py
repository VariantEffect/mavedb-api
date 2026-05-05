import enum


class ScoreCalibrationRelation(str, enum.Enum):
    threshold = "threshold"
    evidence = "evidence"
    method = "method"
