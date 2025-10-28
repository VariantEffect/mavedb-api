import logging
from typing import Optional, Union

from biocommons.seqrepo import SeqRepo
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from mavedb import deps
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.seqrepo import get_sequence_ids, seqrepo_versions, sequence_generator
from mavedb.view_models.seqrepo import SeqRepoMetadata, SeqRepoVersions

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/seqrepo",
    tags=["Seqrepo"],
    responses={404: {"description": "not found"}},
    route_class=LoggedRoute,
)


@router.get("/sequence/{alias}")
def get_sequence(
    alias: str,
    start: Optional[int] = Query(None),
    end: Optional[int] = Query(None),
    sr: SeqRepo = Depends(deps.get_seqrepo),
) -> StreamingResponse:
    save_to_logging_context(
        {
            "requested_seqrepo_alias": alias,
            "requested_seqrepo_start": start,
            "requested_seqrepo_end": end,
            "requested_resource": "sequence",
        }
    )

    # Validate coordinates
    if start is not None and end is not None and start > end:
        logger.error(msg="Invalid coordinates: start is greater than end.", extra=logging_context())
        raise HTTPException(status_code=422, detail="Invalid coordinates: start > end")

    # Only allow one matching sequence for the given alias
    seq_ids = get_sequence_ids(sr, alias)
    save_to_logging_context({"seqrepo_sequence_ids": len(seq_ids)})
    if not seq_ids:
        logger.error(msg="Sequence not found", extra=logging_context())
        raise HTTPException(status_code=404, detail="Sequence not found")
    if len(seq_ids) > 1:
        logger.error(msg="Multiple sequences found for alias", extra=logging_context())
        raise HTTPException(status_code=422, detail=f"Multiple sequences exist for alias '{alias}'")

    return StreamingResponse(sequence_generator(sr, seq_ids[0], start, end), media_type="text/plain")


@router.get("/metadata/{alias}", response_model=SeqRepoMetadata)
def get_metadata(alias: str, sr: SeqRepo = Depends(deps.get_seqrepo)) -> dict[str, Union[str, list[str]]]:
    save_to_logging_context({"requested_seqrepo_alias": alias, "requested_resource": "metadata"})

    seq_ids = get_sequence_ids(sr, alias)
    save_to_logging_context({"seqrepo_sequence_ids": len(seq_ids)})
    if not seq_ids:
        logger.error(msg="Sequence not found", extra=logging_context())
        raise HTTPException(status_code=404, detail="Sequence not found")
    if len(seq_ids) > 1:
        logger.error(msg="Multiple sequences found for alias", extra=logging_context())
        raise HTTPException(status_code=422, detail=f"Multiple sequences exist for alias '{alias}'")

    seq_id = seq_ids[0]
    seq_info = sr.sequences.fetch_seqinfo(seq_id)
    aliases = sr.aliases.find_aliases(seq_id=seq_id)

    return {
        "added": seq_info["added"],
        "aliases": [f"{alias['namespace']}:{alias['alias']}" for alias in aliases],
        "alphabet": seq_info["alpha"],
        "length": seq_info["len"],
    }


@router.get("/version", response_model=SeqRepoVersions)
def get_versions() -> dict[str, str]:
    return seqrepo_versions()
