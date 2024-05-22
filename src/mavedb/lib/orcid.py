import logging
import os
from typing import Optional

import orcid # type: ignore

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
    try:
        record = api.read_record_public(orcid_id, "record", search_token)
        try:
            email = record["person"]["emails"]["email"][0]["email"]
        except:
            # The ORCID user exists, but the email address is not visible.
            pass
    except Exception as e:
        logger.warn("Error while looking up an ORCID user's email address (ORCID ID: %s): %s", orcid_id, e)
    return email
