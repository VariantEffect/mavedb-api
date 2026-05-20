import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import requests

from mavedb.lib.exceptions import HGNCGeneNotFoundError, HGNCServiceError
from mavedb.lib.hgnc.constants import HGNC_REST_BASE_URL
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context

logger = logging.getLogger(__name__)

HGNC_FETCH_HEADERS = {"Accept": "application/json"}
HGNC_FETCH_TIMEOUT = 10


def _optional_doc_string(doc: dict, key: str) -> Optional[str]:
    value = doc.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        logger.error(msg=f"HGNC REST response contained malformed {key}.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")
    return value


@dataclass
class HGNCGeneInfo:
    symbol: str
    name: str
    hgnc_id: Optional[str] = None
    locus_group: Optional[str] = None
    location: Optional[str] = None
    omim_id: Optional[str] = None


def fetch_gene_info(symbol: str) -> HGNCGeneInfo:
    quoted_symbol = quote(symbol, safe="")
    url = f"{HGNC_REST_BASE_URL}/fetch/symbol/{quoted_symbol}"
    save_to_logging_context({"hgnc_symbol": symbol})

    try:
        response = requests.get(url, headers=HGNC_FETCH_HEADERS, timeout=HGNC_FETCH_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as exc:
        save_to_logging_context(format_raised_exception_info_as_dict(exc))
        logger.error(msg="HGNC REST request failed.", exc_info=exc, extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable") from exc
    except ValueError as exc:
        save_to_logging_context(format_raised_exception_info_as_dict(exc))
        logger.error(msg="HGNC REST returned invalid JSON.", exc_info=exc, extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable") from exc

    if not isinstance(data, dict):
        logger.error(msg="HGNC REST response was not a JSON object.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    response_data = data.get("response")
    if not isinstance(response_data, dict):
        logger.error(msg="HGNC REST response did not contain expected response object.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    docs = response_data.get("docs")
    if not isinstance(docs, list):
        logger.error(msg="HGNC REST response did not contain expected docs list.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    if not docs:
        raise HGNCGeneNotFoundError(f"Gene symbol not found: {symbol}")

    doc = docs[0]
    if not isinstance(doc, dict):
        logger.error(msg="HGNC REST response contained a malformed gene document.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    doc_symbol = doc.get("symbol", symbol)
    doc_name = doc.get("name")
    if not isinstance(doc_symbol, str) or not doc_symbol or not isinstance(doc_name, str) or not doc_name:
        logger.error(msg="HGNC REST response contained invalid gene identity fields.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    omim_ids = doc.get("omim_id") or []
    if omim_ids and not isinstance(omim_ids, list):
        logger.error(msg="HGNC REST response contained malformed OMIM identifiers.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")
    if omim_ids and not isinstance(omim_ids[0], str):
        logger.error(msg="HGNC REST response contained malformed OMIM identifiers.", extra=logging_context())
        raise HGNCServiceError("Gene information service temporarily unavailable")

    return HGNCGeneInfo(
        symbol=doc_symbol,
        name=doc_name,
        hgnc_id=_optional_doc_string(doc, "hgnc_id"),
        locus_group=_optional_doc_string(doc, "locus_group"),
        location=_optional_doc_string(doc, "location"),
        omim_id=omim_ids[0] if omim_ids else None,
    )
