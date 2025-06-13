import logging
import requests
import os
from datetime import datetime
from typing import Optional
from urllib import parse


from jose import jwt

from mavedb.lib.logging.context import logging_context, save_to_logging_context, format_raised_exception_info_as_dict
from mavedb.lib.clingen.constants import GENBOREE_ACCOUNT_NAME, GENBOREE_ACCOUNT_PASSWORD, LDH_MAVE_ACCESS_ENDPOINT

from mavedb.lib.types.clingen import LdhSubmission
from mavedb.lib.utils import batched

logger = logging.getLogger(__name__)


class ClinGenLdhService:
    """
    A service class for interacting with the ClinGen Linked Data Hub (LDH) API.

    This class provides methods for authenticating with the Genboree services and dispatching
    submissions to the ClinGen LDH API.

    Attributes:
        url (str): The base URL of the ClinGen LDH API.

    Methods:
        __init__(url: str) -> None:
            Initializes the ClinGenLdhService instance with the given API URL.

        authenticate() -> str:
            Authenticates with the Genboree services and retrieves a JSON Web Token (JWT).
            If a valid JWT already exists, it is reused. Otherwise, a new JWT is obtained
            by authenticating with the Genboree API.

        dispatch_submissions(content_submissions: list[LdhSubmission], batch_size: Optional[int] = None) -> tuple[list, list]:
            Dispatches a list of LDH submissions to the ClinGen LDH API. Supports optional
            batching of submissions.

            Args:
                content_submissions (list[LdhSubmission]): A list of LDH submissions to be dispatched.
                batch_size (Optional[int]): The size of each batch for submission. If None, no batching is applied.

            Returns:
                tuple[list, list]: A tuple containing two lists:
                    - A list of successful submission responses.
                    - A list of failed submissions.

        _existing_jwt() -> Optional[str]:
            Checks for an existing and valid Genboree JWT in the environment variables.

            Returns:
                Optional[str]: The existing JWT if valid, or None if no valid JWT is found.
    """

    def __init__(self, url: str) -> None:
        self.url = url

    def authenticate(self) -> str:
        """
        Authenticates with Genboree services and retrieves a JSON Web Token (JWT).

        This method first checks for an existing JWT using the `_existing_jwt` method. If a valid JWT is found,
        it is returned immediately. Otherwise, the method attempts to authenticate with Genboree services
        using the account name and password provided via environment variables.

        Raises:
            ValueError: If the Genboree account name or password is not set, or if the JWT cannot be parsed
                        from the authentication response.
            requests.exceptions.HTTPError: If the HTTP request to Genboree services fails.

        Returns:
            str: The JWT retrieved from Genboree services, which is also stored in the `GENBOREE_JWT`
                 environment variable for future use.
        """
        if existing_jwt := self._existing_jwt():
            logger.debug(msg="Using existing Genboree JWT for authentication.", extra=logging_context())
            return existing_jwt

        logger.debug(
            msg="No existing or valid Genboree JWT found. Authenticating via Genboree services.",
            extra=logging_context(),
        )

        auth_url = f"https://genboree.org/auth/usr/gb:{GENBOREE_ACCOUNT_NAME}/auth"
        auth_body = {"type": "plain", "val": GENBOREE_ACCOUNT_PASSWORD}
        auth_response = requests.post(auth_url, json=auth_body)
        try:
            auth_response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            save_to_logging_context(format_raised_exception_info_as_dict(exc))
            logger.error(msg="Failed to authenticate with Genboree services.", exc_info=exc, extra=logging_context())
            raise exc

        auth_jwt = auth_response.json().get("data", {}).get("jwt")

        try:
            assert auth_jwt is not None, "No JWT in response."
        except AssertionError as exc:
            msg = "Failed to authenticate with Genboree services. Could not parse JWT from valid response."
            save_to_logging_context(format_raised_exception_info_as_dict(exc))
            logger.error(msg=msg, extra=logging_context())
            raise ValueError(msg)

        # TODO#411: We should consider using a secret manager to store persistent/setable secrets like this.
        #           I'd prefer not to ever set environment variables, especially externally generated content.
        os.environ["GENBOREE_JWT"] = auth_jwt
        logger.info(msg="Successfully authenticated with Genboree services.", extra=logging_context())
        return auth_jwt

    def dispatch_submissions(
        self, content_submissions: list[LdhSubmission], batch_size: Optional[int] = None
    ) -> tuple[list, list]:
        """
        Dispatches a list of content submissions to a specified URL in batches, if specified.

        Args:
            content_submissions (list[LdhSubmission]): A list of submissions to be dispatched.
            batch_size (Optional[int]): The size of each batch for dispatching submissions.
                If None, submissions are dispatched without batching.

        Returns:
            tuple[list, list]: A tuple containing two lists:
                - The first list contains the successful submission responses.
                - The second list contains the submissions that failed to dispatch.

        Raises:
            requests.exceptions.RequestException: If an error occurs during the HTTP request.
        """
        submission_successes = []
        submission_failures = []
        submissions = list(batched(content_submissions, batch_size)) if batch_size is not None else content_submissions
        save_to_logging_context({"ldh_submission_count": len(content_submissions)})

        if batch_size is not None:
            save_to_logging_context({"ldh_submission_batch_size": batch_size})
            save_to_logging_context({"ldh_submission_batch_count": len(submissions)})
            logger.debug("Batching ldh submissions.", extra=logging_context())

        logger.info(msg=f"Dispatching {len(submissions)} ldh submissions...", extra=logging_context())
        for idx, content in enumerate(submissions):
            try:
                logger.debug(msg=f"Dispatching submission {idx+1}.", extra=logging_context())
                response = requests.put(
                    url=self.url,
                    json=content,
                    headers={"Authorization": f"Bearer {self.authenticate()}", "Content-Type": "application/json"},
                )
                response.raise_for_status()
                submission_successes.append(response.json())
                logger.info(
                    msg=f"Successfully dispatched ldh submission ({idx+1} / {len(submissions)}).",
                    extra=logging_context(),
                )

            except requests.exceptions.RequestException as exc:
                save_to_logging_context(format_raised_exception_info_as_dict(exc))
                logger.error(msg="Failed to dispatch ldh submission.", exc_info=exc, extra=logging_context())
                submission_failures.append(content)
                continue

        save_to_logging_context(
            {
                "ldh_submission_success_count": len(submission_successes),
                "ldh_submission_failure_count": len(submission_failures),
            }
        )
        logger.info(msg="Done dispatching ldh submissions.", extra=logging_context())
        return submission_successes, submission_failures

    def _existing_jwt(self) -> Optional[str]:
        """
        Checks for an existing Genboree JWT (JSON Web Token) in the environment variables.

        This method retrieves the JWT from the "GENBOREE_JWT" environment variable, verifies its
        presence, and checks its expiration status. If the token is valid and not expired, it is returned.
        Otherwise, it returns None.

        Returns:
            Optional[str]: The existing and valid Genboree JWT if found, otherwise None.
        """
        logger.debug(msg="Checking for existing Genboree JWT.", extra=logging_context())

        existing_jwt = os.getenv("GENBOREE_JWT")

        if not existing_jwt:
            logger.debug(msg="No existing Genboree JWT was set.", extra=logging_context())
            return None

        expiration = jwt.get_unverified_claims(existing_jwt).get("exp", datetime.now().timestamp())

        if expiration > datetime.now().timestamp():
            logger.debug(msg="Found existing and valid Genboree JWT.", extra=logging_context())
            return existing_jwt

        logger.debug(msg="Found existing but expired Genboree JWT.", extra=logging_context())
        return None


def get_clingen_variation(urn: str) -> Optional[dict]:
    """
    Fetches ClinGen variation data for a given URN (Uniform Resource Name) from the Linked Data Hub.

    Args:
        urn (str): The URN of the variation to fetch.

    Returns:
        Optional[dict]: A dictionary containing the variation data if the request is successful,
                        or None if the request fails.
    """
    response = requests.get(
        f"{LDH_MAVE_ACCESS_ENDPOINT}/{parse.quote_plus(urn)}",
        headers={"Accept": "application/json"},
    )

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch data for URN {urn}: {response.status_code} - {response.text}")
        return None


def clingen_allele_id_from_ldh_variation(variation: Optional[dict]) -> Optional[str]:
    """
    Extracts the ClinGen allele ID from a given variation dictionary.

    Args:
        variation (Optional[dict]): A dictionary containing variation data, otherwise None.

    Returns:
        Optional[str]: The ClinGen allele ID if found, otherwise None.
    """
    if not variation:
        return None

    try:
        return variation["data"]["ldFor"]["Variant"][0]["entId"]
    except (KeyError, IndexError) as exc:
        save_to_logging_context(format_raised_exception_info_as_dict(exc))
        logger.error("Failed to extract ClinGen allele ID from variation data.", extra=logging_context())
        return None
