from _typeshed import Incomplete
from abc import ABC
from collections.abc import Generator
from enum import Enum
from ..core.identifiers import PrevVrsVersion as PrevVrsVersion
from ..core.models import Entity as Entity, iriReference as IRI
from pydantic import BaseModel, RootModel, StringConstraints as StringConstraints, ValidationInfo as ValidationInfo
from typing import Annotated, Literal

def flatten(vals) -> Generator[Incomplete, None, Incomplete]: ...
def flatten_type(t): ...
def overlaps(a: list, b: list): ...
def pydantic_class_refatt_map(): ...

class VrsType(str, Enum):
    LEN_EXPR = "LengthExpression"
    REF_LEN_EXPR = "ReferenceLengthExpression"
    LIT_SEQ_EXPR = "LiteralSequenceExpression"
    SEQ_REF = "SequenceReference"
    SEQ_LOC = "SequenceLocation"
    ALLELE = "Allele"
    CIS_PHASED_BLOCK = "CisPhasedBlock"
    ADJACENCY = "Adjacency"
    TERMINUS = "Terminus"
    TRAVERSAL_BLOCK = "TraversalBlock"
    DERIVATIVE_MOL = "DerivativeMolecule"
    CN_COUNT = "CopyNumberCount"
    CN_CHANGE = "CopyNumberChange"

class Orientation(str, Enum):
    FORWARD = "forward"
    REVERSE_COMPLEMENT = "reverse_complement"

class ResidueAlphabet(str, Enum):
    AA = "aa"
    NA = "na"

class CopyChange(str, Enum):
    COMPLETE_GENOMIC_LOSS = "complete genomic loss"
    HIGH_LEVEL_LOSS = "high-level loss"
    LOW_LEVEL_LOSS = "low-level loss"
    LOSS = "loss"
    REGIONAL_BASE_PLOIDY = "regional base ploidy"
    GAIN = "gain"
    LOW_LEVEL_GAIN = "low-level gain"
    HIGH_LEVEL_GAIN = "high-level gain"

class Syntax(str, Enum):
    HGVS_C = "hgvs.c"
    HGVS_P = "hgvs.p"
    HGVS_G = "hgvs.g"
    HGVS_M = "hgvs.m"
    HGVS_N = "hgvs.n"
    HGVS_R = "hgvs.r"
    HGVS_ISCN = "iscn"
    GNOMAD = "gnomad"
    SPDI = "spdi"

class _ValueObject(Entity, ABC):
    def __hash__(self): ...
    def ga4gh_serialize(self) -> dict: ...
    class ga4gh:
        keys: list[str]

    @staticmethod
    def is_ga4gh_identifiable(): ...

class _Ga4ghIdentifiableObject(_ValueObject, ABC):
    type: str
    digest: Annotated[str, None] | None = None
    def __lt__(self, other): ...
    @staticmethod
    def is_ga4gh_identifiable(): ...
    def has_valid_ga4gh_id(self): ...
    def compute_digest(self, store: bool = True, as_version: PrevVrsVersion | None = None) -> str: ...
    id: Incomplete
    def get_or_create_ga4gh_identifier(
        self, in_place: str = "default", recompute: bool = False, as_version: Incomplete | None = None
    ) -> str: ...
    def compute_ga4gh_identifier(self, recompute: bool = False, as_version: Incomplete | None = None): ...
    def get_or_create_digest(self, recompute: bool = False) -> str: ...
    class ga4gh(_ValueObject.ga4gh):
        prefix: str

class Expression(BaseModel):
    syntax: Syntax
    value: str
    syntax_version: str | None = None

class Range(RootModel):
    root: list[int | None]
    def validate_range(cls, v: list[int | None]) -> list[int | None]: ...

class Residue(RootModel):
    root: Annotated[str, None]

class SequenceString(RootModel):
    root: Annotated[str, None]

class LengthExpression(_ValueObject):
    type: Literal["LengthExpression"] = "LengthExpression"
    length: Range | int | None = None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class ReferenceLengthExpression(_ValueObject):
    type: Literal["ReferenceLengthExpression"] = "ReferenceLengthExpression"
    length: Range | int
    sequence: SequenceString | None = None
    repeatSubunitLength: int
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class LiteralSequenceExpression(_ValueObject):
    type: Literal["LiteralSequenceExpression"] = "LiteralSequenceExpression"
    sequence: SequenceString
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class SequenceReference(_ValueObject):
    type: Literal["SequenceReference"] = "SequenceReference"
    refgetAccession: Annotated[str, None]
    residueAlphabet: ResidueAlphabet | None = None
    circular: bool | None = None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class SequenceLocation(_Ga4ghIdentifiableObject):
    type: Literal["SequenceLocation"] = "SequenceLocation"
    sequenceReference: IRI | SequenceReference | None = None
    start: Range | int | None = None
    end: Range | int | None = None
    sequence: SequenceString | None = None
    def validate_start_end(cls, v: Range | int | None, info: ValidationInfo) -> Range | int | None: ...
    def ga4gh_serialize_as_version(self, as_version: PrevVrsVersion): ...
    def get_refget_accession(self): ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        priorPrefix: Incomplete
        keys: Incomplete

class _VariationBase(_Ga4ghIdentifiableObject, ABC):
    expressions: list[Expression] | None = None

class Allele(_VariationBase):
    type: Literal["Allele"] = "Allele"
    location: IRI | SequenceLocation
    state: LiteralSequenceExpression | ReferenceLengthExpression | LengthExpression
    def ga4gh_serialize_as_version(self, as_version: PrevVrsVersion): ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        priorPrefix: Incomplete
        keys: Incomplete

class CisPhasedBlock(_VariationBase):
    type: Literal["CisPhasedBlock"] = "CisPhasedBlock"
    members: list[Allele | IRI]
    sequenceReference: SequenceReference | None = None
    def ga4gh_serialize(self) -> dict: ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class Adjacency(_VariationBase):
    type: Literal["Adjacency"] = "Adjacency"
    adjoinedSequences: list[IRI | SequenceLocation]
    linker: LiteralSequenceExpression | ReferenceLengthExpression | LengthExpression | None = None
    homology: bool | None = None
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class Terminus(_VariationBase):
    type: Literal["Terminus"] = "Terminus"
    location: IRI | SequenceLocation
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class TraversalBlock(_ValueObject):
    type: Literal["TraversalBlock"] = "TraversalBlock"
    orientation: Orientation | None = None
    component: Allele | CisPhasedBlock | Adjacency | Terminus | None = None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class DerivativeMolecule(_VariationBase):
    type: Literal["DerivativeMolecule"] = "DerivativeMolecule"
    components: list[IRI | TraversalBlock]
    circular: bool | None = None
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class _CopyNumber(_VariationBase, ABC):
    location: IRI | SequenceLocation

class CopyNumberCount(_CopyNumber):
    type: Literal["CopyNumberCount"] = "CopyNumberCount"
    copies: Range | int
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class CopyNumberChange(_CopyNumber):
    type: Literal["CopyNumberChange"] = "CopyNumberChange"
    copyChange: CopyChange
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class MolecularVariation(RootModel):
    root: Allele | CisPhasedBlock | Adjacency | Terminus | DerivativeMolecule

class SequenceExpression(RootModel):
    root: LiteralSequenceExpression | ReferenceLengthExpression | LengthExpression

class Location(RootModel):
    root: SequenceLocation

class Variation(RootModel):
    root: Allele | CisPhasedBlock | Adjacency | Terminus | DerivativeMolecule | CopyNumberChange | CopyNumberCount

class SystemicVariation(RootModel):
    root: CopyNumberChange | CopyNumberCount

reffable_classes: Incomplete
union_reffable_classes: Incomplete
class_refatt_map: Incomplete
class_keys: Incomplete
