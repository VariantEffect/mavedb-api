from abc import ABC
from enum import Enum
from pydantic import BaseModel, RootModel, StringConstraints as StringConstraints
from typing import Annotated, Any, Literal

class CoreImType(str, Enum):
    AGENT: str
    CONTRIBUTION: str
    DOCUMENT: str
    METHOD: str
    DATA_SET: str
    EVIDENCE_LINE: str
    INFORMATION_ENTITY: str
    STUDY_GROUP: str

class Relation(str, Enum):
    CLOSE_MATCH: str
    EXACT_MATCH: str
    BROAD_MATCH: str
    NARROW_MATCH: str
    RELATED_MATCH: str

class AgentSubtype(str, Enum):
    PERSON: str
    ORGANIZATION: str
    SOFTWARE: str

class Direction(str, Enum):
    SUPPORTS: str
    NEUTRAL: str
    DISPUTES: str

class Code(RootModel):
    root: Annotated[str, None]

class IRI(RootModel):
    def __hash__(self): ...
    def ga4gh_serialize(self): ...
    root: str

class Coding(BaseModel):
    label: str | None = None
    system: str
    systemVersion: str | None = None
    code: Code

class ConceptMapping(BaseModel):
    coding: Coding
    relation: Relation

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

class DomainEntity(Entity, ABC):
    mappings: list[ConceptMapping] | None = None

class Agent(Entity):
    type: Literal["Agent"] = "Agent"
    name: str | None = None
    subtype: AgentSubtype | None = None

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
    specifiedBy: Method | IRI | None = None
    contributions: list[Contribution] | None = None
    reportedIn: list[Document | IRI] | None = None
    dateAuthored: str | None = None
    recordMetadata: RecordMetadata | None = None

class InformationEntity(InformationEntityBase):
    derivedFrom: list[InformationEntity] | None = None

class Document(InformationEntity):
    type: Literal["Document"] = "Document"  # type: ignore
    subtype: Coding | None = None
    title: str | None = None
    urls: list[Annotated[str, None]] | None = None
    doi: Annotated[str, None] | None = None
    pmid: int | None = None

class Method(InformationEntity):
    type: Literal["Method"] = "Method"  # type: ignore
    subtype: Coding | None = None
    license: str | None = None

class RecordMetadata(BaseModel):
    recordIdentifier: str | None = None
    recordVersion: str | None = None
    derivedFrom: str | None = None
    dateRecordCreated: str | None = None
    contributions: list[Contribution] | None = None

class DataSet(InformationEntity):
    type: Literal["DataSet"] = "DataSet"  # type: ignore
    subtype: Coding | None = None
    releaseDate: str | None = None
    version: str | None = None
    license: str | None = None

class EvidenceLine(InformationEntity):
    type: Literal["EvidenceLine"] = "EvidenceLine"  # type: ignore
    hasEvidenceItems: list[InformationEntity] | None = None
    directionOfEvidenceProvided: Direction | None = None
    strengthOfEvidenceProvided: Coding | IRI | None = None
    scoreOfEvidenceProvided: float | None = None

class StatementBase(InformationEntity, ABC):
    predicate: str
    direction: Direction | None = None
    strength: Coding | IRI | None = None
    score: float | None = None
    statementText: str | None = None
    classification: Coding | IRI | None = None
    hasEvidenceLines: list[EvidenceLine] | None = None

class Statement(StatementBase):
    subject: dict
    object: dict

class StudyGroup(Entity):
    type: Literal["StudyGroup"] = "StudyGroup"
    memberCount: int | None = None
    isSubsetOf: list[StudyGroup] | None = None
    characteristics: list[Characteristic] | None = None

class Characteristic(BaseModel):
    name: str
    value: str
    valueOperator: bool | None = None

class StudyResultBase(InformationEntityBase, ABC):
    sourceDataSet: list[DataSet] | None = None
    ancillaryResults: dict | None = None
    qualityMeasures: dict | None = None

class StudyResult(InformationEntityBase, ABC):
    focus: DomainEntity | Coding | IRI | None = None
    sourceDataSet: list[DataSet] | None = None
    componentResult: list[StudyResult] | None = None
    studyGroup: StudyGroup | None = None
    ancillaryResults: dict | None = None
    qualityMeasures: dict | None = None
