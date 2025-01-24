from itertools import chain
from typing import Any

import hgvs.dataproviders.uta
from cdot.hgvs.dataproviders import RESTDataProvider
from fastapi import APIRouter, Depends, HTTPException
from hgvs import parser, validator
from hgvs.exceptions import HGVSDataNotAvailableError, HGVSInvalidVariantError

from mavedb.deps import hgvs_data_provider

router = APIRouter(
    prefix="/api/v1/hgvs",
    tags=["transcripts"],
    responses={404: {"description": "Not found"}},
)


@router.get("/fetch/{accession}", status_code=200, response_model=str)
def hgvs_fetch(accession: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> str:
    """
    List stored sequences
    """
    try:
        return hdp.seqfetcher.fetch_seq(accession)
    except HGVSDataNotAvailableError as e:
        raise HTTPException(404, str(e))


@router.post("/validate", status_code=200, response_model=bool)
def hgvs_validate(variant: dict[str, str], hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> bool:
    """
    Validate a provided variant
    """
    hp = parser.Parser()
    variant_hgvs = hp.parse(variant["variant"])

    try:
        valid = validator.Validator(hdp=hdp).validate(variant_hgvs, strict=False)
    except HGVSInvalidVariantError as e:
        raise HTTPException(400, str(e))
    else:
        return valid


@router.get("/assemblies", status_code=200, response_model=list[str])
def list_assemblies(hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> list[str]:
    """
    List stored assemblies
    """
    return list(hdp.assembly_maps.keys())


@router.get("/{assembly}/accessions", status_code=200, response_model=list[str])
def list_accessions(assembly: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> list[str]:
    """
    List stored accessions
    """
    if assembly not in hdp.assembly_maps:
        raise HTTPException(404, f"Assembly '{assembly}' Not Found")

    return list(hdp.get_assembly_map(assembly_name=assembly).keys())


@router.get("/genes", status_code=200, response_model=list)
def list_genes():
    """
    List stored genes
    """
    # Even though it doesn't provide the most complete transcript pool, UTA does provide more direct
    # access to a complete list of genes which have transcript information available.
    return list(chain.from_iterable(hgvs.dataproviders.uta.connect()._fetchall("select hgnc from gene")))


@router.get("/genes/{gene}", status_code=200, response_model=dict[str, Any])
def gene_info(gene: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> dict[str, Any]:
    """
    List stored gene information for a specified gene
    """
    gene_info = hdp.get_gene_info(gene)

    if not gene_info:
        raise HTTPException(404, f"No gene info found for {gene}.")

    return gene_info


@router.get("/gene/{gene}", status_code=200, response_model=list[str])
def list_transcripts_for_gene(gene: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> list[str]:
    """
    List transcripts associated with a particular gene
    """
    transcripts = set([tx_info["tx_ac"] for tx_info in hdp.get_tx_for_gene(gene)])

    if not transcripts:
        raise HTTPException(404, f"No associated transcripts found for {gene}.")

    return list(transcripts)


@router.get("/{transcript}", status_code=200, response_model=dict[str, Any])
def transcript_info(transcript: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> dict[str, Any]:
    """
    List transcript information for a particular transcript
    """
    transcript_info = hdp.get_tx_identity_info(transcript)

    if not transcript_info:
        raise HTTPException(404, f"No transcript information found for {transcript}.")

    return transcript_info


@router.get("/protein/{transcript}", status_code=200, response_model=str)
def convert_to_protein(transcript: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> str:
    """
    Convert a provided transcript from it's nucleotide accession identifier to its protein accession identifier
    """
    protein_transcript = hdp.get_pro_ac_for_tx_ac(transcript)

    if not protein_transcript:
        raise HTTPException(404, f"No protein transcript found for {transcript}.")

    return protein_transcript
