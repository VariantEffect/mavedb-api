import os
from typing import Optional

from cdot.hgvs.dataproviders import SeqFetcher, ChainedSeqFetcher, FastaSeqFetcher, RESTDataProvider

from mavedb.lib.mapping import VRSMap

GENOMIC_FASTA_FILES = [
    "/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz",
    "/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz",
]

DCD_MAP_URL = os.environ.get("DCD_MAPPING_URL", "http://dcd-mapping:8000")


def seqfetcher() -> ChainedSeqFetcher:
    return ChainedSeqFetcher(SeqFetcher(), *[FastaSeqFetcher(file) for file in GENOMIC_FASTA_FILES])


def cdot_rest() -> RESTDataProvider:
    return RESTDataProvider(seqfetcher=seqfetcher())


def vrs_mapper(url: Optional[str] = None) -> VRSMap:
    return VRSMap(DCD_MAP_URL) if not url else VRSMap(url)
