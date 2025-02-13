from enum import Enum
from ga4gh.core.entity_models import (
    Coding as Coding,
    DataSet as DataSet,
    IRI as IRI,
    Method as Method,
    StatementBase as StatementBase,
    StudyGroup as StudyGroup,
    StudyResult as StudyResult,
    StudyResultBase as StudyResultBase,
)
from abc import ABC
from ga4gh.vrs.models import MolecularVariation as MolecularVariation
from typing import Literal

class AveFunctionalClassification(str, Enum):
    NORMAL: str
    INDETERMINATE: str
    ABNORMAL: str

class AveClinicalClassification(str, Enum):
    PS3_STRONG: str
    PS3_MODERATE: str
    PS3_SUPPORTING: str
    BS3_STRONG: str
    BS3_MODERATE: str
    BS3_SUPPORTING: str

class AssayVariantEffectFunctionalClassificationStatement(StatementBase, ABC):
    type: Literal["AssayVariantEffectFunctionalClassificationStatement"] = (
        "AssayVariantEffectFunctionalClassificationStatement"  # type: ignore
    )
    subjectVariant: MolecularVariation | IRI
    predicate: Literal["hasAssayVariantEffectFor"] = "hasAssayVariantEffectFor"
    objectAssay: IRI | Coding
    classification: AveFunctionalClassification  # type: ignore
    specifiedBy: Method | IRI | None = None

class AssayVariantEffectClinicalClassificationStatement(StatementBase, ABC):
    type: Literal["AssayVariantEffectClinicalClassificationStatement"] = (
        "AssayVariantEffectClinicalClassificationStatement"  # type: ignore
    )
    subjectVariant: MolecularVariation | IRI
    predicate: Literal["hasAssayVariantEffectFor"] = "hasAssayVariantEffectFor"
    objectAssay: IRI | Coding
    classification: AveClinicalClassification  # type: ignore
    specifiedBy: Method | IRI | None = None

class AssayVariantEffectMeasurementStudyResult(StudyResultBase, ABC):
    type: Literal["AssayVariantEffectMeasurementStudyResult"] = "AssayVariantEffectMeasurementStudyResult"  # type: ignore
    componentResult: list[StudyResult] | None = None
    studyGroup: StudyGroup | None = None
    focusVariant: MolecularVariation | IRI | None = None
    score: float | None = None
    specifiedBy: Method | IRI | None = None
    sourceDataSet: list[DataSet] | None = None
