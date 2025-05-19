from enum import Enum

class MembershipOperator(str, Enum):
    AND = "AND"
    OR = "OR"

class StrengthOfEvidenceProvided(str, Enum):
    STANDALONE = "standalone"
    VERY_STRONG = "very strong"
    STRONG = "strong"
    MODERATE = "moderate"
    SUPPORTING = "supporting"
