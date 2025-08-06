import requests
import time
import logging

from requests.adapters import HTTPAdapter, Retry
from typing import List, Optional, Dict, Any

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.uniprot.constants import UNIPROT_ID_MAPPING_API_URL, SWISS_PROT_ENTRY_TYPE
from mavedb.lib.types import uniprot

logger = logging.getLogger(__name__)


class UniProtIDMappingAPI:
    """
    A class to interact with the UniProt REST API for ID mapping.

    See: https://www.uniprot.org/help/id_mapping_prog
    """

    API_URL = UNIPROT_ID_MAPPING_API_URL
    polling_interval: int
    polling_tries: int

    def __init__(self, polling_interval: int = 3, polling_tries: int = 5):
        self.polling_interval = polling_interval
        self.polling_tries = polling_tries

        retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def submit_id_mapping(self, from_db: str, to_db: str, ids: List[str]) -> Optional[str]:
        """
        Submits a request to map identifiers from one database to another using the UniProt ID mapping API.

        Args:
            from_db (str): The source database from which the identifiers originate.
            to_db (str): The target database to which the identifiers should be mapped.
            ids (List[str]): A list of identifiers to be mapped.

        Returns:
            str: The job ID assigned by the UniProt API for tracking the mapping request.

        Raises:
            requests.HTTPError: If the API request fails or returns an error status code.
        """
        if not ids:
            logger.warning(msg="No IDs provided for UniProt ID mapping. Skipped submission.", extra=logging_context())
            return None

        save_to_logging_context({"from_db": from_db, "to_db": to_db, "ids": ids})
        logger.debug(msg=f"Submitting Uniprot ID mapping for {len(ids)} IDs.", extra=logging_context())

        resp = self.session.post(
            f"{self.API_URL}/idmapping/run",
            data={"from": from_db, "to": to_db, "ids": ",".join(ids)},
        )
        resp.raise_for_status()

        if "jobId" not in resp.json():
            logger.error(msg=f"Response from UniProt does not contain jobId: {resp.json()}", extra=logging_context())
            return None

        save_to_logging_context({"uniprot_job_id": resp.json()["jobId"]})
        logger.debug(msg="Submitted new Uniprot ID mapping job.", extra=logging_context())
        return resp.json()["jobId"]

    def check_id_mapping_results_ready(self, job_id: str) -> bool:
        """
        Checks if the ID mapping job with the given job_id has finished processing.

        This method polls the UniProt API for the status of the specified job, waiting up to a maximum number of tries.
        If the job status becomes "FINISHED" within the allowed polling attempts, the method returns True.
        Otherwise, it waits for a specified interval between each poll and returns False if the job does not finish in time.

        Args:
            job_id (str): The identifier of the ID mapping job to check.

        Returns:
            bool: True if the job status is "FINISHED" within the polling attempts, False otherwise.
        """
        save_to_logging_context(
            {
                "uniprot_job_id": job_id,
                "polling_interval": self.polling_interval,
                "max_polling_attempts": self.polling_tries,
            }
        )
        logger.debug(msg="Checking status of UniProt ID mapping job.", extra=logging_context())

        for attempt in range(self.polling_tries):
            save_to_logging_context({"polling_attempt": attempt + 1})
            logger.debug(msg="Polling status for Uniprot mapping job.", extra=logging_context())

            resp = self.session.get(f"{self.API_URL}/idmapping/status/{job_id}")
            resp.raise_for_status()
            status_response = resp.json()

            if "jobStatus" in status_response:
                save_to_logging_context({"uniprot_job_status": status_response["jobStatus"]})

                if status_response["jobStatus"] == "FINISHED":
                    logger.info(msg=f"UniProt ID mapping job {job_id} finished successfully.", extra=logging_context())
                    return True
                else:
                    logger.info(
                        msg="UniProt ID mapping job has not finished successfully. Retrying after polling interval.",
                        extra=logging_context(),
                    )
                    time.sleep(self.polling_interval)

            # If the response already contains results or failed IDs, we can consider it as finished.
            else:
                return bool(status_response.get("results") or status_response.get("failedIds"))

        logger.warning(
            msg="UniProt ID mapping job did not finish within the allowed attempts.", extra=logging_context()
        )
        return False

    def get_id_mapping_results(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieve the results of a UniProt ID mapping job.

        Given a job ID, this method queries the UniProt API for the status and results of an ID mapping job.
        It first fetches the job details to obtain a redirect URL, then retrieves the mapping results from that URL.

        Args:
            job_id (str): The unique identifier for the UniProt ID mapping job.

        Returns:
            Dict[str, Any]: The JSON response containing the mapping results.

        Raises:
            requests.HTTPError: If any of the HTTP requests fail.
            KeyError: If the expected 'redirectURL' key is missing in the response.
        """
        save_to_logging_context({"uniprot_job_id": job_id})
        logger.debug(msg=f"Fetching results for UniProt ID mapping job {job_id}.", extra=logging_context())

        result_response = self.session.get(f"{self.API_URL}/idmapping/details/{job_id}")
        result_response.raise_for_status()

        redirect_url = result_response.json()["redirectURL"]
        save_to_logging_context({"uniprot_redirect_url": redirect_url})
        logger.debug(msg=f"Redirect URL for UniProt ID mapping job: {redirect_url}", extra=logging_context())

        results_resp = self.session.get(redirect_url)
        results_resp.raise_for_status()
        return results_resp.json()

    @staticmethod
    def extract_uniprot_id_from_results(
        results: Dict[str, Any], prefer_swiss_prot: bool = True
    ) -> uniprot.MappingEntries:
        """
        Extracts UniProt ID mappings from a results dictionary. Retains only mappings with a 5/5 annotation score from

        Args:
            results (Dict[str, Any]): A dictionary containing a "results" key, which is a list of mapping result dictionaries. Each mapping result should contain a "from" key (source ID) and a "to" key (a dictionary with a "primaryAccession" key for the UniProt ID).

        Returns:
            list[dict[str, str]]: A list of dictionaries, each mapping a source ID to its corresponding UniProt primary accession ID.
        """
        uniprot_mappings: uniprot.MappingEntries = []
        swiss_prot_mappings: uniprot.MappingEntries = []
        uniprot_results = results.get("results", [])

        save_to_logging_context({"total_uniprot_results": len(uniprot_results), "prefer_swiss_prot": prefer_swiss_prot})
        logger.debug(msg="Extracting UniProt ID mappings from results.", extra=logging_context())

        for r in uniprot_results:
            from_id = r.get("from")
            to_id = r.get("to", {}).get("primaryAccession")
            entry_type = r.get("to", {}).get("entryType")

            if from_id and to_id:
                uniprot_mappings.append({from_id: {"uniprot_id": to_id, "entry_type": entry_type}})
            else:
                save_to_logging_context({"skipped_uniprot_mapping_result": r})
                logger.warning(
                    msg="Skipping mapping for result due to missing 'from' or 'to' ID.", extra=logging_context()
                )

        save_to_logging_context({"total_uniprot_mappings": len(uniprot_mappings)})

        if prefer_swiss_prot:
            swiss_prot_mappings = [
                mapping
                for mapping in uniprot_mappings
                if mapping[next(iter(mapping))]["entry_type"] == SWISS_PROT_ENTRY_TYPE
            ]

            save_to_logging_context({"total_swiss_prot_mappings": len(swiss_prot_mappings)})
            logger.debug(
                msg="Swiss-Prot mappings were preferred. Done extracting Swiss-Prot mappings.", extra=logging_context()
            )

        if prefer_swiss_prot and not swiss_prot_mappings and uniprot_mappings:
            logger.warning(
                msg="No Swiss-Prot mappings found when Swiss-Prot was preferred, falling back to all UniProt mappings.",
                extra=logging_context(),
            )
            return uniprot_mappings
        elif not prefer_swiss_prot:
            logger.debug(
                msg="Swiss-Prot preference is disabled. Returning all UniProt mappings.", extra=logging_context()
            )
            return uniprot_mappings

        logger.debug(msg="Returning Swiss-Prot mappings.", extra=logging_context())
        return swiss_prot_mappings
