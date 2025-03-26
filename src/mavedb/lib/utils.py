import logging
import requests
import time


logger = logging.getLogger(__name__)


def request_with_backoff(
    method: str, url: str, backoff_limit: int = 5, backoff_wait: int = 10, **kwargs
) -> requests.Response:
    attempt = 0
    while attempt <= backoff_limit:
        logger.debug(f"Attempting request to {url}. This is attempt {attempt+1}.")
        try:
            response = requests.request(method=method, url=url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Request to {url} failed.", exc_info=exc)
            backoff_time = backoff_wait * (2**attempt)
            attempt += 1
            logger.info(f"Retrying request to {url} in {backoff_wait} seconds.")
            time.sleep(backoff_time)

    raise requests.exceptions.RequestException(f"Request to {url} failed after {backoff_limit} attempts.")
