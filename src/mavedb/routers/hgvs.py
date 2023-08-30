from typing import Any, Union

import hgvs.validator
import hgvs.parser
import hgvs.dataproviders.uta
import hgvs.dataproviders.seqfetcher
import hgvs.dataproviders.interface
from fastapi import APIRouter, HTTPException
import hgvs
import bioutils.assemblies

router = APIRouter(
    prefix="/api/v1/hgvs",
    tags=["hgvs"],
    responses={404: {"description": "Not found"}},
)


@router.get("/fetch/{seq_id}", status_code=200, response_model=Any)
def hgvs_fetch(seq_id: str) -> Any:
    """
    List stored sequences
    """
    return hgvs.dataproviders.uta.connect().seqfetcher.fetch_seq(seq_id)


@router.post("/validate", status_code=200, response_model=Any)
def hgvs_validate(variant: str) -> Any:
    """
    List stored sequences
    """
    hp = hgvs.parser.Parser()
    hdp = hgvs.dataproviders.uta.connect()

    try:
        valid = hgvs.validator.Validator(hdp=hdp).validate(hp.parse(variant), strict=False)
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


@router.get("/{assembly}/accessions", status_code=200, response_model=list[str])
def list_accessions(assembly: str) -> Any:
    """
    List stored accessions
    """
    return list(hgvs.dataproviders.uta.connect().get_assembly_map(assembly_name=assembly).keys())
