from _typeshed import Incomplete
from abc import ABC
from collections.abc import Generator
from enum import Enum
from ga4gh.core.identifiers import PrevVrsVersion as PrevVrsVersion
from ga4gh.core.entity_models import Entity as Entity, IRI as IRI
from pydantic import BaseModel, RootModel, StringConstraints as StringConstraints, ValidationInfo as ValidationInfo
from typing import Annotated, Literal

def flatten(vals) -> Generator[Incomplete, None, Incomplete]: ...
def flatten_type(t): ...
def overlaps(a: list, b: list): ...
def pydantic_class_refatt_map(): ...

class VrsType(str, Enum):
    LEN_EXPR: str
    REF_LEN_EXPR: str
    LIT_SEQ_EXPR: str
    SEQ_REF: str
    SEQ_LOC: str
    ALLELE: str
    CIS_PHASED_BLOCK: str
    ADJACENCY: str
    TERMINUS: str
    TRAVERSAL_BLOCK: str
    DERIVATIVE_MOL: str
    CN_COUNT: str
    CN_CHANGE: str

class Orientation(str, Enum):
    FORWARD: str
    REVERSE_COMPLEMENT: str

class ResidueAlphabet(str, Enum):
    AA: str
    NA: str

class CopyChange(str, Enum):
    EFO_0030069: str
    EFO_0020073: str
    EFO_0030068: str
    EFO_0030067: str
    EFO_0030064: str
    EFO_0030070: str
    EFO_0030071: str
    EFO_0030072: str

class Syntax(str, Enum):
    HGVS_C: str
    HGVS_P: str
    HGVS_G: str
    HGVS_M: str
    HGVS_N: str
    HGVS_R: str
    HGVS_ISCN: str
    GNOMAD: str
    SPDI: str

class _ValueObject(Entity, ABC):
    def __hash__(self): ...
    def ga4gh_serialize(self) -> dict: ...
    class ga4gh:
        keys: list[str]

    @staticmethod
    def is_ga4gh_identifiable(): ...

class _Ga4ghIdentifiableObject(_ValueObject, ABC):
    type: str
    digest: Annotated[str, None] | None
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
    syntax_version: str | None

class Range(RootModel):
    root: list[int | None]
    def validate_range(cls, v: list[int | None]) -> list[int | None]: ...

class Residue(RootModel):
    root: Annotated[str, None]

class SequenceString(RootModel):
    root: Annotated[str, None]

class LengthExpression(_ValueObject):
    type: Literal["LengthExpression"]
    length: Range | int | None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class ReferenceLengthExpression(_ValueObject):
    type: Literal["ReferenceLengthExpression"]
    length: Range | int
    sequence: SequenceString | None
    repeatSubunitLength: int
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class LiteralSequenceExpression(_ValueObject):
    type: Literal["LiteralSequenceExpression"]
    sequence: SequenceString
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class SequenceReference(_ValueObject):
    type: Literal["SequenceReference"]
    refgetAccession: Annotated[str, None]
    residueAlphabet: ResidueAlphabet | None
    circular: bool | None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class SequenceLocation(_Ga4ghIdentifiableObject):
    type: Literal["SequenceLocation"]
    sequenceReference: IRI | SequenceReference | None
    start: Range | int | None
    end: Range | int | None
    sequence: SequenceString | None
    def validate_start_end(cls, v: Range | int | None, info: ValidationInfo) -> Range | int | None: ...
    def ga4gh_serialize_as_version(self, as_version: PrevVrsVersion): ...
    def get_refget_accession(self): ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        priorPrefix: Incomplete
        keys: Incomplete

class _VariationBase(_Ga4ghIdentifiableObject, ABC):
    expressions: list[Expression] | None

class Allele(_VariationBase):
    type: Literal["Allele"]
    location: IRI | SequenceLocation
    state: LiteralSequenceExpression | ReferenceLengthExpression | LengthExpression
    def ga4gh_serialize_as_version(self, as_version: PrevVrsVersion): ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        priorPrefix: Incomplete
        keys: Incomplete

class CisPhasedBlock(_VariationBase):
    type: Literal["CisPhasedBlock"]
    members: list[Allele | IRI]
    sequenceReference: SequenceReference | None
    def ga4gh_serialize(self) -> dict: ...
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class Adjacency(_VariationBase):
    type: Literal["Adjacency"]
    adjoinedSequences: list[IRI | SequenceLocation]
    linker: LiteralSequenceExpression | ReferenceLengthExpression | LengthExpression | None
    homology: bool | None
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class Terminus(_VariationBase):
    type: Literal["Terminus"]
    location: IRI | SequenceLocation
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class TraversalBlock(_ValueObject):
    type: Literal["TraversalBlock"]
    orientation: Orientation | None
    component: Allele | CisPhasedBlock | Adjacency | Terminus | None
    class ga4gh(_ValueObject.ga4gh):
        keys: Incomplete

class DerivativeMolecule(_VariationBase):
    type: Literal["DerivativeMolecule"]
    components: list[IRI | TraversalBlock]
    circular: bool | None
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class _CopyNumber(_VariationBase, ABC):
    location: IRI | SequenceLocation

class CopyNumberCount(_CopyNumber):
    type: Literal["CopyNumberCount"]
    copies: Range | int
    class ga4gh(_Ga4ghIdentifiableObject.ga4gh):
        prefix: str
        keys: Incomplete

class CopyNumberChange(_CopyNumber):
    type: Literal["CopyNumberChange"]
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
