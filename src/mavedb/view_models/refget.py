from typing import List, Optional
from pydantic import BaseModel, Field


class RefgetAlias(BaseModel):
    naming_authority: str = Field(..., description="Name of the authority, which issued the given alias")
    alias: str = Field(..., description="Free text alias for a given sequence")


class RefgetMetadata(BaseModel):
    id: str = Field(..., description="Unique sequence identifier")
    MD5: Optional[str] = Field(None, description="MD5 checksum of the reference sequence.")
    trunc512: Optional[str] = Field(
        None,
        description="Truncated, to 48 characters, SHA-512 checksum of the reference sequence encoded as a HEX string. No longer a preferred serialisation of the SHA-512.",
    )
    ga4gh: Optional[str] = Field(
        ...,
        description="A ga4gh identifier used to identify the sequence. This is a [base64url](defined in RFC4648 Â§5) representation of the 1st 24 bytes from a SHA-512 digest of normalised sequence. This is the preferred method of representing the SHA-512 sequence digest.",
    )
    length: int = Field(..., description="A decimal integer of the length of the reference sequence.")
    aliases: List[RefgetAlias] = Field(..., description="List of aliases for the sequence.")


class RefgetMetadataResponse(BaseModel):
    metadata: RefgetMetadata


class RefgetImplementationInfo(BaseModel):
    identifier_types: List[str] = Field(
        ...,
        description="An array of strings listing the type of name space that are supported.",
    )
    algorithms: List[str] = Field(
        ...,
        description="An array of strings listing the digest algorithms that are supported.",
    )
    circular_supported: bool = Field(..., description="Indicates if the service supports circular location queries.")
    subsequence_limit: Optional[int] = Field(
        None,
        description="Maximum length of sequence which may be requested using start and/or end query parameters.",
    )


class RefgetServiceInfo(BaseModel):
    name: str = Field(..., description="Name of this service.")
    version: str = Field(..., description="Version of this service.")
    seqrepo_dependency_version: str = Field(..., description="Version of the SeqRepo dependency used by this service.")
    seqrepo_data_version: str = Field(..., description="Version of the SeqRepo data instance used by this service.")
    description: str = Field(..., description="Description of this service.")
    refget: RefgetImplementationInfo
