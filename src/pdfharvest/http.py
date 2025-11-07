from __future__ import annotations
import asyncio
import logging
import uuid
import aiofiles
from pathlib import Path
from typing import Any, Dict, Optional


import httpx  

CROSSREF = "https://api.crossref.org/works/"
UNPAYWALL = "https://api.unpaywall.org/v2/"

"""
    Perform an HTTP request with exponential backoff retry logic.

    Args:
        client (httpx.AsyncClient)
        method (str)
        url (str)
        max_tries (int, optional)
        base (float, optional)
        cap (float, optional)
  

    Returns:
        httpx.Response: Successful response object.
"""


async def backoff_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    log = logging.getLogger("pdfharvest.http")
    max_tries = 6
    base = 0.5
    cap = 10.0

    for i in range(max_tries):
        try:
            r = await client.request(method, url, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(base * (2 ** i), cap)
                log.warning(f"{r.status_code} {url} → retry in {wait:.2f}s")
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except httpx.RequestError as e:
            if i == max_tries - 1:
                log.error(f"HTTP error: {e}")
                raise
            wait = min(base * (2 ** i), cap)
            log.warning(f"Transport error: {e} → retry in {wait:.2f}s")
            await asyncio.sleep(wait)
    raise RuntimeError("backoff_request exhausted retries")


"""
    Retrieve metadata for a DOI from the Crossref API.

    Args:
        client (httpx.AsyncClient)
        doi (str)

    Returns:
        dict: Metadata record from Crossref if available, or an empty dict if not found.
"""
async def fetch_crossref(client: httpx.AsyncClient, doi: str) -> Dict[str, Any]:
    log = logging.getLogger("pdfharvest.http")
    url = f"{CROSSREF}{doi}"
    try:
        r = await backoff_request(client, "GET", url, timeout=20)
        data = r.json()
        return data.get("message", {}) if isinstance(data, dict) else {}
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            log.warning(f"DOI not found in Crossref: {doi}")
            return {}
        else:
            raise  
    except Exception as e:
        log.error(f"Error fetching Crossref for {doi}: {e}")
        return {}


"""
    Retrieve open access metadata for a DOI from the Unpaywall API.

    Args:
        client (httpx.AsyncClient)
        doi (str)
        email (str)

    Returns:
        dict: Metadata from Unpaywall including open access links and status.
"""
async def fetch_unpaywall(client: httpx.AsyncClient, doi: str, email: str) -> Dict[str, Any]:
    log = logging.getLogger("pdfharvest.http")
    url = f"{UNPAYWALL}{doi}?email={email}"
    try: 
        r = await backoff_request(client, "GET", url, params={"email": email}, timeout=20)
        if r.status_code == 404:
            return {}
        return r.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            log.warning(f"DOI not found in Crossref: {doi}")
            return {}
        else:
            raise  
    except Exception as e:
        log.error(f"Error fetching Crossref for {doi}: {e}")
        return {}



"""
    Choose the best PDF URL from a list of candidate links based on heuristics.

    Args:
        candidates (list[str])
        doi (str, optional)
        prefer_direct (bool, optional)

    Returns:
        str | None: The selected URL expected to point to a PDF, or None if no suitable candidate was found.
"""
def best_pdf_url(ua: Dict[str, Any]) -> Optional[str]:
    if not ua:
        return None
    loc = ua.get("best_oa_location") or {}
    pdf = loc.get("url_for_pdf") or loc.get("url")
    if pdf:
        return pdf
    for loc in ua.get("oa_locations") or []:
        pdf = loc.get("url_for_pdf") or loc.get("url")
        if pdf:
            return pdf
    return None


"""
    Download a PDF file from the given URL and save it to a local path.

    Args:
        client (httpx.AsyncClient)
        url (str)
        dest_path (Path)

    Returns:
        bool: True if the file was downloaded successfully, False otherwise.

    Raises:
        httpx.RequestError: If the network request fails.
"""
async def download_pdf(client: httpx.AsyncClient, url: str, out_path) -> bool:
    try:
        temp_path = out_path.with_stem(out_path.stem + "_" + str(uuid.uuid4())[:8])

        async with client.stream("GET", url, follow_redirects=True, timeout=30) as r:
            if r.status_code != 200:
                return False

            content_type = r.headers.get("Content-Type", "").lower()
            if "pdf" not in content_type:
                print(f"Not a valid PDF: {url} (type={content_type})")
                return False

            async with aiofiles.open(temp_path, "wb") as f:
                async for chunk in r.aiter_bytes():
                    await f.write(chunk)

        temp_path.replace(out_path)
        return True

    except Exception as e:
        print(f"PDF download failed {url}: {e}")
        return False
