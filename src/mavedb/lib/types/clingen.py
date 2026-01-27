from typing import Any, Literal, Optional, TypedDict

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


## Allele Registry Response Types

# For many of these objects, typing has been omitted for brevity. Details about field contents are
# available in the ClinGen Allele Registry documentation: https://reg.clinicalgenome.org/doc/AlleleRegistry_1.01.xx_api_v1.pdf
# and should be added as needed.


class ClinGenExternalRecord(TypedDict):
    dbSnp: NotRequired[list[dict[str, Any]]]
    ClinVarAlleles: NotRequired[list[dict[str, Any]]]
    ClinVarVariations: NotRequired[list[dict[str, Any]]]
    MyVariantInfo_hg38: NotRequired[list[dict[str, Any]]]
    MyVariantInfo_hg19: NotRequired[list[dict[str, Any]]]
    ExAC: NotRequired[list[dict[str, Any]]]
    gnomAD: NotRequired[list[dict[str, Any]]]
    COSMIC: NotRequired[list[dict[str, Any]]]


class ClinGenAlleleDefinition(TypedDict):
    hgvs: list[str]
    referenceSequence: str
    gene: NotRequired[str]
    geneSymbol: NotRequired[str]
    geneId: NotRequired[int]
    coordinates: NotRequired[dict[str, Any]]
    referenceGeneome: NotRequired[Literal["GRCh37", "GRCh38", "NCBI36"]]
    chromosome: NotRequired[
        Literal[
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "X",
            "Y",
            "MT",
        ]
    ]


ClinGenAllele = TypedDict(
    "ClinGenAllele",
    {
        "@id": str,
        "type": Literal["nucleotide", "amino-acid"],
        "activeUris": list[str],
        "externalRecords": NotRequired[list[ClinGenExternalRecord]],
        "externalSources": NotRequired[dict[str, Any]],
        "genomicAlleles": NotRequired[list[ClinGenAlleleDefinition]],
        "transcriptAlleles": NotRequired[list[ClinGenAlleleDefinition]],
        "aminoAcidAlleles": NotRequired[list[ClinGenAlleleDefinition]],
    },
)

ClinGenSubmissionError = TypedDict(
    "ClinGenSubmissionError",
    {
        "description": str,
        "errorType": str,
        "hgvs": str,
        "inputLine": str,
        "message": str,
        "position": str,
    },
)
