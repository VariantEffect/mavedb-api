import logging
import time

import requests

logger = logging.getLogger(__name__)


def request_with_backoff(
    method: str, url: str, backoff_limit: int = 5, backoff_wait: int = 10, **kwargs
) -> requests.Response:
    attempt = 0
    while attempt <= backoff_limit:
        logger.debug(f"Attempting request to {url}. This is attempt {attempt + 1}.")
        try:
            response = requests.request(method=method, url=url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            # 429 means rate-limited — retrying with backoff is the correct response.
            # Other 4xx errors are client mistakes that won't improve on retry.
            if status is not None and status != 429 and status < 500:
                raise
            logger.warning(f"Request to {url} failed with status {status}.", exc_info=exc)
            backoff_time = backoff_wait * (2**attempt)
            attempt += 1
            logger.info(f"Retrying request to {url} in {backoff_wait} seconds.")
            time.sleep(backoff_time)
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Request to {url} failed.", exc_info=exc)
            backoff_time = backoff_wait * (2**attempt)
            attempt += 1
            logger.info(f"Retrying request to {url} in {backoff_wait} seconds.")
            time.sleep(backoff_time)

    raise requests.exceptions.RequestException(f"Request to {url} failed after {backoff_limit} attempts.")


# TODO: When we upgrade to Python 3.12, we can replace this with the built-in `itertools.batched` method.
def batched(iterable, n):
    """
    Yield successive n-sized chunks from iterable.
    """
    l = len(iterable)  # noqa: E741
    for i in range(0, l, n):
        yield iterable[i : min((i + n, l))]
