import os
from typing import TYPE_CHECKING, Optional

import boto3
from cdot.hgvs.dataproviders import ChainedSeqFetcher, FastaSeqFetcher, RESTDataProvider, SeqFetcher

from mavedb.lib.mapping import VRSMap

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

GENOMIC_FASTA_FILES = [
    "/data/GCF_000001405.39_GRCh38.p13_genomic.fna.gz",
    "/data/GCF_000001405.25_GRCh37.p13_genomic.fna.gz",
]

DCD_MAP_URL = os.environ.get("DCD_MAPPING_URL", "http://dcd-mapping:8000")
CDOT_URL = os.environ.get("CDOT_URL", "http://cdot-rest:8000")
CSV_UPLOAD_S3_BUCKET_NAME = os.getenv("UPLOAD_S3_BUCKET_NAME", "score-set-csv-uploads-dev")


def seqfetcher() -> ChainedSeqFetcher:
    return ChainedSeqFetcher(SeqFetcher(), *[FastaSeqFetcher(file) for file in GENOMIC_FASTA_FILES])


def cdot_rest() -> RESTDataProvider:
    return RESTDataProvider(url=CDOT_URL, seqfetcher=seqfetcher())


def vrs_mapper(url: Optional[str] = None) -> VRSMap:
    return VRSMap(DCD_MAP_URL) if not url else VRSMap(url)


def s3_client() -> "S3Client":
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION_NAME", "us-west-2"),
    )
