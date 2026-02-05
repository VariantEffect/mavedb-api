import enum


class FunctionalClassification(str, enum.Enum):
    normal = "normal"
    abnormal = "abnormal"
    not_specified = "not_specified"
