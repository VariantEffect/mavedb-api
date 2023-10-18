from itertools import chain
from typing import Any, Union

import bioutils.assemblies
import hgvs
import hgvs.dataproviders.interface
import hgvs.dataproviders.seqfetcher
import hgvs.dataproviders.uta
import hgvs.parser
import hgvs.validator
from fastapi import APIRouter, HTTPException

router = APIRouter(
    prefix="/api/v1/hgvs",
    tags=["hgvs"],
    responses={404: {"description": "Not found"}},
)


@router.get("/fetch/{accession}", status_code=200, response_model=Any)
def hgvs_fetch(accession: str) -> Any:
    """
    List stored sequences
    """
    return hgvs.dataproviders.uta.connect().seqfetcher.fetch_seq(accession)


@router.post("/validate", status_code=200, response_model=Any)
def hgvs_validate(variant: dict[str, str]) -> Any:
    """
    List stored sequences
    """
    hp = hgvs.parser.Parser()
    hdp = hgvs.dataproviders.uta.connect()

    try:
        valid = hgvs.validator.Validator(hdp=hdp).validate(hp.parse(variant["variant"]), strict=False)
    except hgvs.validator.HGVSInvalidVariantError as e:
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
def list_accessions(assembly: str) -> Any:
    """
    List stored accessions
    """
    return list(hgvs.dataproviders.uta.connect().get_assembly_map(assembly_name=assembly).keys())


@router.get("/genes", status_code=200, response_model=list)
def list_genes():
    return list(chain.from_iterable(hgvs.dataproviders.uta.connect()._fetchall("select hgnc from gene")))


@router.get("/genes/{gene}", status_code=200, response_model=list)
def gene_info(gene: str):
    return hgvs.dataproviders.uta.connect().get_gene_info(gene)


@router.get("/transcripts/gene/{gene}", status_code=200, response_model=list)
def list_transcripts_for_gene(gene: str):
    # the 4th element of each tx_info list is the accession number for the transcript. Only
    # surface unique elements
    return set([tx_info[3] for tx_info in hgvs.dataproviders.uta.connect().get_tx_for_gene(gene)])


@router.get("/transcripts/{transcript}", status_code=200, response_model=list)
def transcript_info(transcript: str):
    # the 4th element of each tx_info list is the accession number for the transcript. Only
    # surface unique elements
    return hgvs.dataproviders.uta.connect().get_tx_identity_info(transcript)


@router.get("/transcripts/protein/{transcript}", status_code=200, response_model=str)
def convert_to_protein(transcript: str):
    return hgvs.dataproviders.uta.connect().get_pro_ac_for_tx_ac(transcript)
