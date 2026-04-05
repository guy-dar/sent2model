"""
Shared HTTP helpers for generated scrapers: timeouts, retries, rate-limit handling.

Import from artifact ``scraper.py`` only if you add the skills ``scripts`` directory
to ``sys.path``; otherwise copy the ``get_with_backoff`` body into the scraper.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


def get_with_backoff(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 10.0,
    max_retries: int = 4,
    base_delay: float = 0.75,
    session: Optional[requests.Session] = None,
) -> Optional[requests.Response]:
    """
    GET with exponential backoff on 429 / 503 and transient failures.

    Respects ``Retry-After`` when present (seconds). Adds a small jitter between
    attempts. Returns ``None`` if all attempts fail.
    """
    hdrs = dict(headers or {})
    sess = session or requests.Session()
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            resp = sess.get(url, headers=hdrs, timeout=timeout)
            if resp.status_code == 429 or resp.status_code == 503:
                wait = _retry_after_seconds(resp, base_delay, attempt)
                logger.warning(
                    "HTTP %s for %s — sleeping %.1fs (attempt %s/%s)",
                    resp.status_code,
                    url[:80],
                    wait,
                    attempt + 1,
                    max_retries + 1,
                )
                time.sleep(wait)
                continue
            return resp
        except (requests.RequestException, OSError) as e:
            last_exc = e
            wait = base_delay * (2**attempt) + random.uniform(0, 0.25)
            logger.warning(
                "Request error for %s: %s — sleeping %.1fs",
                url[:80],
                e,
                wait,
            )
            time.sleep(wait)
    if last_exc:
        logger.error("Giving up on %s after %s attempts: %s", url[:80], max_retries + 1, last_exc)
    return None


def _retry_after_seconds(resp: requests.Response, base_delay: float, attempt: int) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return float(ra)
        except ValueError:
            pass
    return base_delay * (2**attempt) + random.uniform(0, 0.35)


def sleep_between_requests(seconds: float) -> None:
    """Call between consecutive HTTP calls inside one ``scrape()`` to stay polite."""
    if seconds > 0:
        time.sleep(seconds)
