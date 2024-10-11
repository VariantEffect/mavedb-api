import logging
import os
from typing import Optional

import orcid  # type: ignore

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.view_models.orcid import OrcidUser

logger = logging.getLogger(__name__)

ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET")
ORCID_JWT_SIGNING_PUBLIC_KEY = os.getenv("ORCID_JWT_SIGNING_PUBLIC_KEY")


def fetch_orcid_user(orcid_id: str) -> Optional[OrcidUser]:
    """
    Given an ORCID ID, fetch the ORCID user's name.
    """
    api = orcid.PublicAPI(ORCID_CLIENT_ID, ORCID_CLIENT_SECRET)
    search_token = api.get_search_token_from_orcid()
    logger.debug(msg="Attempting to fetch user from ORCID.", extra=logging_context())
    try:
        record = api.read_record_public(orcid_id, "record", search_token)
        try:
            user = OrcidUser(
                orcid_id=record["orcid-identifier"]["path"],
                given_name=record["person"]["name"]["given-names"]["value"],
                family_name=record["person"]["name"]["family-name"]["value"],
            )
            logger.debug(msg="Successfully fetched ORCID user.", extra=logging_context())
        except Exception:
            logger.debug(
                msg="Failed to fetch ORCID user; User exists but is name metadata not visible.", extra=logging_context()
            )

    except Exception as e:
        save_to_logging_context({"orcid_exception": str(e)})
        logger.warn(msg="Encountered an error while looking up an ORCID user's email address.", extra=logging_context())
        return None

    return user


def fetch_orcid_user_email(orcid_id: str) -> Optional[str]:
    """
    Given an ORCID ID, fetch the ORCID user's name.

    The default visibility for email addresses on ORCID is `Only me`, so MaveDB will usually not be able to get the
    address.
    """

    api = orcid.PublicAPI(ORCID_CLIENT_ID, ORCID_CLIENT_SECRET)
    search_token = api.get_search_token_from_orcid()
    email: Optional[str] = None
    logger.debug(msg="Attempting to fetch user email from ORCID.", extra=logging_context())
    try:
        record = api.read_record_public(orcid_id, "record", search_token)
        try:
            email = record["person"]["emails"]["email"][0]["email"]
            logger.debug(msg="Successfully fetched ORCID email.", extra=logging_context())
        except Exception:
            logger.debug(msg="Failed to fetch ORCID email; User exists but email not visible.", extra=logging_context())

    except Exception as e:
        save_to_logging_context({"orcid_exception": str(e)})
        logger.warn(msg="Encountered an error while looking up an ORCID user's email address.", extra=logging_context())

    return email
