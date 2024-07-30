import logging
import os
from typing import Optional

import orcid  # type: ignore

from mavedb.lib.logging.context import dump_context, save_to_context

logger = logging.getLogger(__name__)

ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET")


def fetch_orcid_user_email(orcid_id: str) -> Optional[str]:
    """
    Given an ORCID ID, fetch the ORCID user's name.

    The default visibility for email addresses on ORCID is `Only me`, so MaveDB will usually not be able to get the
    address.
    """

    api = orcid.PublicAPI(ORCID_CLIENT_ID, ORCID_CLIENT_SECRET)
    search_token = api.get_search_token_from_orcid()
    email: Optional[str] = None
    logger.debug(f"Attempting to fetch user email from ORCID. {dump_context()}")
    try:
        record = api.read_record_public(orcid_id, "record", search_token)
        try:
            email = record["person"]["emails"]["email"][0]["email"]
            logger.debug(f"Successfully fetched ORCID email. {dump_context()}")
        except:
            logger.debug(f"Failed to fetch ORCID email; User exists but email not visible. {dump_context()}")

    except Exception as e:
        save_to_context({"orcid_exception": str(e)})
        logger.warn(f"Encountered an error while looking up an ORCID user's email address. {dump_context()}")

    return email
