import os
from datetime import date
from typing import Optional, TypedDict

import requests
from cdot.hgvs.dataproviders import ChainedSeqFetcher, FastaSeqFetcher, RESTDataProvider

GENOMIC_FASTA_FILES = [
    "/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz",
    "/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz",
]

DCD_MAP_URL = os.environ.get("DCD_MAPPING_URL", "http://dcd-mapping:8000")


def seqfetcher() -> ChainedSeqFetcher:
    return ChainedSeqFetcher(*[FastaSeqFetcher(file) for file in GENOMIC_FASTA_FILES])


def cdot_rest() -> RESTDataProvider:
    return RESTDataProvider(seqfetcher=seqfetcher())


class VRSMap:
    url: str

    class ScoreSetMappingResults(TypedDict):
        metadata: Optional[dict[str, str]]
        dcd_mapping_version: str
        mapped_date_utc: date
        computed_genomic_reference_sequence: Optional[dict[str, str]]
        mapped_genomic_reference_sequence: Optional[dict[str, str]]
        computed_protein_reference_sequence: Optional[dict[str, str]]
        mapped_protein_reference_sequence: Optional[dict[str, str]]
        mapped_scores: Optional[list[dict]]
        error_message: Optional[str]

    def __init__(self, url: str) -> None:
        self.url = url

    def map_score_set(self, score_set_urn: str) -> ScoreSetMappingResults:
        uri = f"{self.url}/api/v1/map/{score_set_urn}"
        response = requests.post(uri)
        response.raise_for_status()
        return response.json()


def vrs_mapper(url: Optional[str] = None) -> VRSMap:
    return VRSMap(DCD_MAP_URL) if not url else VRSMap(url)
