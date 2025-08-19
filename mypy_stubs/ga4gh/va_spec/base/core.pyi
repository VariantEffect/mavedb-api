from _typeshed import Incomplete
from abc import ABC
from datetime import date
from enum import Enum
from pydantic import BaseModel, RootModel, StringConstraints as StringConstraints
from typing import Annotated, Any, Literal, TypeVar

from ...core.models import Coding, MappableConcept
from ...vrs.models import MolecularVariation

StatementType = TypeVar("StatementType")
EvidenceLineType = TypeVar("EvidenceLineType")

class CoreImType(str, Enum):
    AGENT = str
    CONTRIBUTION = str
    DOCUMENT = str
    METHOD = str
    DATA_SET = str
    EVIDENCE_LINE = str
    INFORMATION_ENTITY = str
    STUDY_GROUP = str

class Direction(str, Enum):
    SUPPORTS = str
    NEUTRAL = str
    DISPUTES = str

class Code(RootModel):
    root: Annotated[str, None]

class iriReference(RootModel):
    def __hash__(self): ...
    def ga4gh_serialize(self): ...
    root: str

class Extension(BaseModel):
    name: str
    value: float | str | bool | dict[str, Any] | list[Any] | None = None
    description: str | None = None

class Entity(BaseModel, ABC):
    id: str | None = None
    type: str
    label: str | None = None
    description: str | None = None
    alternativeLabels: list[str] | None = None
    extensions: list[Extension] | None = None

class Agent(Entity):
    type: Literal["Agent"] = "Agent"
    name: str | None = None

class ActivityBase(Entity, ABC):
    subtype: Coding | None = None
    date: str | None = None
    specifiedBy: list[Method] | None = None
    @classmethod
    def date_format(cls, v: str | None) -> str | None: ...

class Activity(ActivityBase):
    performedBy: list[Agent] | None = None

class Contribution(ActivityBase):
    type: Literal["Contribution"] = "Contribution"
    contributor: list[Agent] | None = None
    activityType: Coding | None = None

class InformationEntityBase(Entity, ABC):
    type: Literal["InformationEntity"] = "InformationEntity"
    specifiedBy: Method | iriReference | None = None
    contributions: list[Contribution] | None = None
    reportedIn: list[Document | iriReference] | None = None
    dateAuthored: str | None = None
    recordMetadata: RecordMetadata | None = None

class InformationEntity(InformationEntityBase):
    derivedFrom: list[InformationEntity] | None = None

class Document(InformationEntity):
    type: Literal["Document"] = "Document"
    subtype: Coding | None = None
    title: str | None = None
    urls: list[Annotated[str, None]] | None = None
    doi: Annotated[str, None] | None = None
    pmid: int | None = None

class Method(InformationEntity):
    type: Literal["Method"] = "Method"
    subtype: Coding | None = None
    license: str | None = None

class RecordMetadata(BaseModel):
    recordIdentifier: str | None = None
    recordVersion: str | None = None
    derivedFrom: str | None = None
    dateRecordCreated: str | None = None
    contributions: list[Contribution] | None = None

class DataSet(Entity):
    type: Literal["DataSet"] = "DataSet"
    subtype: MappableConcept | None = None
    reportedIn: Document | iriReference | None = None
    releaseDate: date | None = None
    version: str | None = None
    license: MappableConcept | None = None

class EvidenceLine(InformationEntity):
    type: Literal["EvidenceLine"] = "EvidenceLine"
    targetProposition: Proposition | SubjectVariantProposition | None = None
    hasEvidenceItems: list[StudyResult | Statement | EvidenceLine | iriReference] | None = None
    directionOfEvidenceProvided: Direction
    strengthOfEvidenceProvided: MappableConcept | None = None
    scoreOfEvidenceProvided: float | None = None
    evidenceOutcome: MappableConcept | None = None

class Proposition(Entity):
    subject: dict
    predicate: str
    object: dict

class Statement(InformationEntity):
    type: Literal["Statement"] = "Statement"
    proposition: (
        ExperimentalVariantFunctionalImpactProposition
        | VariantDiagnosticProposition
        | VariantOncogenicityProposition
        | VariantPathogenicityProposition
        | VariantPrognosticProposition
        | VariantTherapeuticResponseProposition
    )
    direction: Direction
    strength: MappableConcept | None = None
    score: float | None = None
    classification: MappableConcept | None = None
    hasEvidenceLines: list[EvidenceLine | iriReference] | None = None

class StudyResult(RootModel):
    root: CohortAlleleFrequencyStudyResult | ExperimentalVariantFunctionalImpactStudyResult

class _StudyResult(InformationEntity, ABC):
    sourceDataSet: DataSet | None = None
    ancillaryResults: dict | None = None
    qualityMeasures: dict | None = None

class _SubjectVariantPropositionBase(Entity, ABC):
    subjectVariant: MolecularVariation | CategoricalVariant | iriReference

class SubjectVariantProposition(RootModel):
    root: (
        ExperimentalVariantFunctionalImpactProposition
        | VariantPathogenicityProposition
        | VariantDiagnosticProposition
        | VariantPrognosticProposition
        | VariantOncogenicityProposition
        | VariantTherapeuticResponseProposition
    )

class ClinicalVariantProposition(_SubjectVariantPropositionBase):
    geneContextQualifier: MappableConcept | iriReference | None = None
    alleleOriginQualifier: MappableConcept | iriReference | None = None

class VariantPathogenicityProposition(ClinicalVariantProposition):
    type: Literal["VariantPathogenicityProposition"] = "VariantPathogenicityProposition"
    targetProposition: VariantPathogenicityProposition | None = None
    strengthOfEvidenceProvided: Coding | iriReference | None = None
    specifiedBy: Method | iriReference | None = None
    hasEvidenceLines: list[EvidenceLine] | None = None

class ExperimentalVariantFunctionalImpactStudyResult(_StudyResult):
    type: Literal["ExperimentalVariantFunctionalImpactStudyResult"] = "ExperimentalVariantFunctionalImpactStudyResult"
    focusVariant: MolecularVariation | iriReference
    functionalImpactScore: float | None = None
    specifiedBy: Method | iriReference | None = None
    sourceDataSet: DataSet | None = None

class ExperimentalVariantFunctionalImpactProposition(_SubjectVariantPropositionBase):
    type: Literal["ExperimentalVariantFunctionalImpactProposition"] = "ExperimentalVariantFunctionalImpactProposition"
    predicate: str = "impactsFunctionOf"
    objectSequenceFeature: iriReference | MappableConcept
    experimentalContextQualifier: iriReference | Document | dict | None = None

# TODO#319: When we pull in CatVRS as a dependency, we can fully type this but as of now there is no need.
CategoricalVariant = Incomplete

# Proposition types we don't care about and thus have no need to strongly type.
VariantDiagnosticProposition = Incomplete
VariantOncogenicityProposition = Incomplete
VariantPrognosticProposition = Incomplete
VariantTherapeuticResponseProposition = Incomplete

# Study result types we don't care about and thus have no need to strongly type.
CohortAlleleFrequencyStudyResult = Incomplete
