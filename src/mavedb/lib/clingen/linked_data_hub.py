import logging
import os
from datetime import datetime
from typing import Optional

import requests
from jose import jwt

from mavedb.lib.logging.context import logging_context, save_to_logging_context, format_raised_exception_info_as_dict
from mavedb.lib.clingen.constants import GENBOREE_ACCOUNT_NAME, GENBOREE_ACCOUNT_PASSWORD
from mavedb.lib.types.clingen import LdhSubmission
from mavedb.lib.utils import batched, request_with_backoff

logger = logging.getLogger(__name__)


class ClinGenLdhService:
    def __init__(self, url: str) -> None:
        self.url = url

    def authenticate(self) -> str:
        logger.info(msg="Attempting to use an existing Genboree JWT.", extra=logging_context())

        if existing_jwt := self._existing_jwt():
            logger.info(msg="Using existing Genboree JWT for authentication.", extra=logging_context())
            return existing_jwt

        logger.info(
            msg="No existing or valid Genboree JWT found. Authenticating via Genboree services.",
            extra=logging_context(),
        )

        try:
            assert GENBOREE_ACCOUNT_NAME is not None, "Genboree account name is not set."
            assert GENBOREE_ACCOUNT_PASSWORD is not None, "Genboree account password is not set."
        except AssertionError as exc:
            msg = "Genboree account name and/or password are not set. Unable to authenticate with Genboree services."
            save_to_logging_context(format_raised_exception_info_as_dict(exc))
            logger.error(msg=msg, extra=logging_context())
            raise ValueError(msg)

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
        os.environ["GENBOREE_JWT"] = auth_jwt
        logger.info(msg="Successfully authenticated with Genboree services.", extra=logging_context())
        return auth_jwt

    def dispatch_submissions(
        self, content_submissions: list[LdhSubmission], batch_size: Optional[int] = None
    ) -> tuple[list, list]:
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
                response = request_with_backoff(
                    method="PUT",
                    url=self.url,
                    json=content,
                    headers={"Authorization": f"Bearer {self.authenticate()}", "Content-Type": "application/json"},
                )
                submission_successes.append(response.json())
                logger.debug(
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
        existing_jwt = os.getenv("GENBOREE_JWT")

        if not existing_jwt:
            logger.debug(msg="No existing Genboree JWT was set.")
            return None

        expiration = jwt.get_unverified_claims(existing_jwt).get("exp", datetime.now().timestamp())

        if expiration > datetime.now().timestamp():
            logger.debug(msg="Found existing and valid Genboree JWT.")
            return existing_jwt

        logger.debug(msg="Found existing but expired Genboree JWT.")
        return None
