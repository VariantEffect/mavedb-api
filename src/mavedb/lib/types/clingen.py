from typing import Optional, TypedDict, Literal
from typing_extensions import NotRequired


# See: https://ldh.genome.network/docs/ldh/submit.html#content-submission-body


### Linked Data Hub Event Type


# The subject of the event (ie. the entity that the event is about)
class EventSbj(TypedDict):
    id: str
    type: str
    format: Literal["hgvs", "alleleRegistryID", "clinvarID", "geneSymbol"]
    add: bool
    iri: Optional[str]


# Who/what triggered the event
class EventTriggerer(TypedDict):
    host: str
    id: str
    iri: str


class EventTrigger(TypedDict):
    by: EventTriggerer
    at: str


class LdhEvent(TypedDict):
    type: str
    name: str
    uuid: str
    sbj: EventSbj
    triggered: EventTrigger


### Linked Data Hub Content Types


# The subject of the content submission
class LdhSubjectVariant(TypedDict):
    id: NotRequired[str]
    hgvs: str


class LdhContentSubject(TypedDict):
    Variant: LdhSubjectVariant


# The entities we are submitting
class LdhMapping(TypedDict):
    mavedb_id: str
    pre_mapped: Optional[str]
    post_mapped: Optional[str]
    mapping_api_version: Optional[str]
    score: float


class LdhEntity(TypedDict):
    entContent: LdhMapping
    entId: str
    entIri: str


class LdhContentLinkedData(TypedDict):
    MaveDBMapping: list[LdhEntity]


### Linked Data Hub Submission Type


class LdhSubmissionContent(TypedDict):
    sbj: LdhContentSubject
    ld: LdhContentLinkedData


class LdhSubmission(TypedDict):
    event: LdhEvent
    content: LdhSubmissionContent
