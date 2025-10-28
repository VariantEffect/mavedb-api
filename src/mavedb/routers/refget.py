"""
Implementation of the Refget API for MaveDB.

See https://ga4gh.github.io/refget/sequences/pub/refget-openapi.yaml for more information
on Refget standards
"""

import logging
import os
import re
from typing import Optional, Union

from biocommons.seqrepo import SeqRepo
from biocommons.seqrepo import __version__ as seqrepo_dep_version
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import StreamingResponse

from mavedb import __version__, deps
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.seqrepo import base64url_to_hex, get_sequence_ids, sequence_generator
from mavedb.routers.shared import (
    BASE_400_RESPONSE,
    BASE_416_RESPONSE,
    BASE_501_RESPONSE,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models.refget import RefgetMetadataResponse, RefgetServiceInfo

RANGE_HEADER_REGEX = r"^bytes=(\d+)-(\d+)$"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/refget",
    tags=["Refget"],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)


@router.get("/sequence/service-info", response_model=RefgetServiceInfo, summary="Get Refget service information")
def service_info() -> dict[str, Union[str, dict[str, Union[str, list[str], bool, None]]]]:
    """
    Returns information about the refget service.
    """
    seqrepo_data_dir = os.getenv("HGVS_SEQREPO_DIR")
    if not seqrepo_data_dir:
        seqrepo_data_version = "unknown"
    else:
        seqrepo_data_version = seqrepo_data_dir.split(os.sep)[-1]  # last part of the path is the version

    return {
        "name": "MaveDB API",
        "version": __version__,
        "seqrepo_dependency_version": seqrepo_dep_version,
        "seqrepo_data_version": seqrepo_data_version,
        "description": "MaveDB API",
        "refget": {
            "identifier_types": ["refseq", "ensembl"],
            "algorithms": ["MD5", "trunc512", "ga4gh"],
            "circular_supported": False,
            "subsequence_limit": None,
        },
    }


@router.get("/sequence/{alias}/metadata", response_model=RefgetMetadataResponse, summary="Get Refget sequence metadata")
def get_metadata(alias: str, sr: SeqRepo = Depends(deps.get_seqrepo)) -> dict[str, dict]:
    """
    Show metadata for a particular Refget sequence with the provided alias.
    """
    save_to_logging_context({"requested_refget_alias": alias, "requested_resource": "metadata"})

    seq_ids = get_sequence_ids(sr, alias)
    save_to_logging_context({"seqrepo_sequence_ids": seq_ids})
    if not seq_ids:
        logger.error(msg="Sequence not found", extra=logging_context())
        raise HTTPException(status_code=404, detail="Sequence not found")
    if len(seq_ids) > 1:
        logger.error(msg="Multiple sequences found for alias", extra=logging_context())
        raise HTTPException(
            status_code=400, detail=f"Multiple sequences exist for alias '{alias}'. Use an explicit namespace"
        )

    seq_id = seq_ids[0]
    seqinfo = sr.sequences.fetch_seqinfo(seq_id)
    aliases = list(sr.aliases.find_aliases(seq_id=seq_id))  # store generator as list so it can be reused

    md5_rec = [a for a in aliases if a["namespace"] == "MD5"]
    md5_id = md5_rec[0]["alias"] if md5_rec else None
    ga4gh_rec = [a for a in aliases if a["namespace"] == "ga4gh"]
    ga4gh_id = ga4gh_rec[0]["alias"] if ga4gh_rec else None

    return {
        "metadata": {
            "id": seq_id,
            "MD5": md5_id,
            "trunc512": base64url_to_hex(seq_id),
            "ga4gh": ga4gh_id,
            "length": seqinfo["len"],
            "aliases": [{"naming_authority": a["namespace"], "alias": a["alias"]} for a in aliases],
        }
    }


@router.get(
    "/sequence/{alias}",
    summary="Get Refget sequence",
    responses={
        200: {"description": "OK: Full sequence returned", "content": {"text/plain": {}}},
        206: {"description": "Partial Content: Partial sequence returned", "content": {"text/plain": {}}},
        **BASE_400_RESPONSE,
        **BASE_416_RESPONSE,
        **BASE_501_RESPONSE,
    },
)
def get_sequence(
    alias: str,
    range_header: Optional[str] = Header(
        None,
        alias="Range",
        description="Specify a substring as a single HTTP Range. One byte range is permitted, "
        "and is 0-based inclusive. For example, 'Range: bytes=0-9' corresponds to '?start=0&end=10'.",
    ),
    start: Optional[int] = Query(None, description="Request a subsequence of the data (0-based)."),
    end: Optional[int] = Query(None, description="Request a subsequence of the data by specifying the end."),
    sr: SeqRepo = Depends(deps.get_seqrepo),
) -> StreamingResponse:
    """
    Get a Refget sequence by alias.
    """
    save_to_logging_context(
        {
            "requested_refget_alias": alias,
            "requested_resource": "sequence",
        }
    )

    if (start is not None or end is not None) and (range_header is not None):
        logger.error(
            msg="Cannot use both start/end query parameters and Range header",
            extra=logging_context(),
        )
        raise HTTPException(status_code=400, detail="Cannot use both start/end query parameters and Range header")

    if range_header:
        m = re.match(RANGE_HEADER_REGEX, range_header)
        if not m:
            logger.error(msg="Invalid range header format", extra=logging_context())
            raise HTTPException(status_code=400, detail="Invalid range header format")

        start, end = int(m.group(1)), int(m.group(2)) + 1

    save_to_logging_context({"requested_refget_start": start, "requested_refget_end": end})
    if start is not None and end is not None:
        if start > end:
            logger.error(
                msg="Invalid coordinates: start is greater than end and circular chromosomes are not supported",
                extra=logging_context(),
            )
            raise HTTPException(
                status_code=501,
                detail="Invalid coordinates: start is greater than end and circular chromosomes are not supported",
            )

    seq_ids = get_sequence_ids(sr, alias)
    if not seq_ids:
        logger.error(msg="Sequence not found", extra=logging_context())
        raise HTTPException(status_code=404, detail="Sequence not found")
    if len(seq_ids) > 1:
        logger.error(msg="Multiple sequences found for alias", extra=logging_context())
        raise HTTPException(
            status_code=400, detail=f"Multiple sequences exist for alias '{alias}'. Use an explicit namespace."
        )

    seq_id = seq_ids[0]
    seqinfo = sr.sequences.fetch_seqinfo(seq_id)

    if start is not None and end is not None:
        if start >= seqinfo["len"]:
            raise HTTPException(
                status_code=416,
                detail="Invalid coordinates: start > sequence length",
                headers={"Content-Range": f"bytes */{seqinfo['len']}"},
            )
        if end > seqinfo["len"]:
            raise HTTPException(
                status_code=416,
                detail="Invalid coordinates: end > sequence length",
                headers={"Content-Range": f"bytes */{seqinfo['len']}"},
            )
        if not (0 <= start <= end <= seqinfo["len"]):
            raise HTTPException(
                status_code=416,
                detail="Invalid coordinates: must obey 0 <= start <= end <= sequence_length",
                headers={"Content-Range": f"bytes */{seqinfo['len']}"},
            )

    headers = {"Content-Length": str(seqinfo["len"])}
    if start is not None and end is not None and range_header:
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end - 1}/{seqinfo['len']}"
        headers["Accept-Ranges"] = "bytes"
    else:
        status = 200
        headers["Accept-Ranges"] = "none"

    return StreamingResponse(
        sequence_generator(sr, seq_ids[0], start, end), media_type="text/plain", status_code=status, headers=headers
    )
