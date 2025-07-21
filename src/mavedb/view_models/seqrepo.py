from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class SeqRepoMetadata(BaseModel):
    added: datetime = Field(..., description="Date the sequence was added to SeqRepo (ISO format)")
    aliases: List[str] = Field(..., description="List of aliases for the sequence, formatted as 'namespace:alias'")
    alphabet: str = Field(..., description="Alphabet of the sequence (e.g., 'ACGMNRT')")
    length: int = Field(..., description="Length of the sequence")


class SeqRepoVersions(BaseModel):
    seqrepo_data_version: str = Field(..., description="Version of the SeqRepo data instance")
    seqrepo_dependency_version: str = Field(..., description="Version of the bioutils.seqrepo dependency")
