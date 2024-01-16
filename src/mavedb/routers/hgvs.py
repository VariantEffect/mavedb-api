from itertools import chain
from typing import Any, Union

import hgvs.dataproviders.uta
import bioutils.assemblies

from cdot.hgvs.dataproviders import RESTDataProvider
from fastapi import APIRouter, Depends, HTTPException
from hgvs import parser, validator

from mavedb.deps import hgvs_data_provider

router = APIRouter(
    prefix="/api/v1/hgvs",
    tags=["hgvs"],
    responses={404: {"description": "Not found"}},
)


@router.get("/fetch/{accession}", status_code=200, response_model=Any)
def hgvs_fetch(accession: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> Any:
    """
    List stored sequences
    """
    return hdp.seqfetcher.fetch_seq(accession)


@router.post("/validate", status_code=200, response_model=Any)
def hgvs_validate(variant: dict[str, str], hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> Any:
    """
    Validate a provided variant
    """
    hp = parser.Parser()
    variant = hp.parse(variant["variant"])

    try:
        valid = validator.Validator(hdp=hdp).validate(variant, strict=False)
    except validator.HGVSInvalidVariantError as e:
        raise HTTPException(400, str(e))
    else:
        return valid


@router.get("/assemblies", status_code=200, response_model=list[str])
def list_assemblies() -> Any:
    """
    List stored assemblies
    """
    return bioutils.assemblies.get_assembly_names()


@router.get("/grouped-assemblies", status_code=200, response_model=list[dict[str, Union[str, list[str]]]])
def list_assemblies_grouped() -> Any:
    """
    List stored assemblies in groups of major/minor versions
    """
    assemblies = list_assemblies()

    major = []
    minor = []
    for assembly in assemblies:
        if "." in assembly:
            minor.append(assembly)
        else:
            major.append(assembly)

    return [
        {"type": "Major Assembly Versions", "assemblies": major},
        {"type": "Minor Assembly Versions", "assemblies": minor},
    ]


@router.get("/{assembly}/accessions", status_code=200, response_model=list)
def list_accessions(assembly: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)) -> Any:
    """
    List stored accessions
    """
    return list(hdp.get_assembly_map(assembly_name=assembly).keys())


@router.get("/genes", status_code=200, response_model=list)
def list_genes():
    """
    List stored genes
    """
    # Even though it doesn't provide the most complete transcript pool, UTA does provide more direct
    # access to a complete list of genes which have transcript information available.
    return list(chain.from_iterable(hgvs.dataproviders.uta.connect()._fetchall("select hgnc from gene")))


@router.get("/genes/{gene}", status_code=200, response_model=list)
def gene_info(gene: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)):
    """
    List stored gene information for a specified gene
    """
    return hdp.get_gene_info(gene)


@router.get("/transcripts/gene/{gene}", status_code=200, response_model=list)
def list_transcripts_for_gene(gene: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)):
    """
    List transcripts associated with a particular gene
    """
    return set([tx_info["tx_ac"] for tx_info in hdp.get_tx_for_gene(gene)])


@router.get("/transcripts/{transcript}", status_code=200, response_model=list)
def transcript_info(transcript: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)):
    """
    List transcript information for a particular transcript
    """
    return hdp.get_tx_identity_info(transcript)


@router.get("/transcripts/protein/{transcript}", status_code=200, response_model=str)
def convert_to_protein(transcript: str, hdp: RESTDataProvider = Depends(hgvs_data_provider)):
    """
    Convert a provided transcript from it's nucleotide accession identifier to its protein accession identifier
    """
    return hdp.get_pro_ac_for_tx_ac(transcript)
