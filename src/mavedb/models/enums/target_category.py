from enum import Enum


class TargetCategory(str, Enum):
    protein_coding = "protein_coding"
    regulatory = "regulatory"
    other_noncoding = "other_noncoding"
