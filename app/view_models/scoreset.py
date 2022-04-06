from datetime import date
from pydantic.types import Optional
from typing import Dict

from app.view_models.base.base import BaseModel
from app.view_models.experiment import Experiment, SavedExperiment
from app.view_models.target_gene import SavedTargetGene, TargetGene
from app.view_models.user import SavedUser, User
from app.view_models.variant import VariantInDbBase


class ScoresetBase(BaseModel):
    urn: Optional[str]
    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Dict
    dataset_columns: Dict
    published_date: Optional[date]
    data_usage_policy: Optional[str]
    licence_id: Optional[int]
    replaces_id: Optional[int]
    keywords: Optional[list[str]]


class ScoresetCreate(ScoresetBase):
    experiment_urn: str
    #target_gene_name: str
    #target_gene_category: str

    #target_gene_ensembl_id_id: Optional[int]
    #target_gene_refseq_id_id: Optional[int]
    #target_gene_uniprot_id_id: Optional[int]

    #target_gene_ensembl_offset: Optional[int]
    #target_gene_refseq_offset: Optional[int]
    #target_gene_uniprot_offset: Optional[int]

    wt_sequence = str


class ScoresetUpdate(ScoresetBase):
    pass


# Properties shared by models stored in DB
class SavedScoreset(ScoresetBase):
    # id: int
    num_variants: int
    experiment: SavedExperiment
    creation_date: date
    modification_date: date
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    target_gene: Optional[SavedTargetGene]  # TODO Make non-optional

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class Scoreset(SavedScoreset):
    experiment: Experiment
    created_by: Optional[User]
    modified_by: Optional[User]
    target_gene: Optional[TargetGene]  # TODO Make non-optional


# Properties to return to clients when variants are requested
class ScoresetWithVariants(SavedScoreset):
    experiment: Experiment
    variants: list[VariantInDbBase]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_gene: Optional[TargetGene]  # TODO Make non-optional


# Properties to return to admin clients
class AdminScoreset(SavedScoreset):
    normalised: bool
    private: bool
    approved: bool
    processing_state: Optional[str]
    experiment: Experiment
    created_by: Optional[User]
    modified_by: Optional[User]
    target_gene: Optional[TargetGene]  # TODO Make non-optional
