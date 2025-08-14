from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.enums import StrengthOfEvidenceProvided

GENERIC_DISEASE_MEDGEN_CODE = "C0012634"
MEDGEN_SYSTEM = "https://www.ncbi.nlm.nih.gov/medgen/"

PILLAR_PROJECT_CALIBRATION_STRENGTH_OF_EVIDENCE_MAP = {
    # No evidence
    0: (VariantPathogenicityEvidenceLine.Criterion.PS3, None),
    # Supporting evidence
    -1: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.SUPPORTING),
    1: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.SUPPORTING),
    # Moderate evidence
    -2: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.MODERATE),
    2: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.MODERATE),
    -3: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.MODERATE),
    3: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.MODERATE),
    # Strong evidence
    -4: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
    4: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
    -5: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
    5: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
    -6: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
    6: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
    -7: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.STRONG),
    7: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.STRONG),
    # Very Strong evidence
    -8: (VariantPathogenicityEvidenceLine.Criterion.BS3, StrengthOfEvidenceProvided.VERY_STRONG),
    8: (VariantPathogenicityEvidenceLine.Criterion.PS3, StrengthOfEvidenceProvided.VERY_STRONG),
}
