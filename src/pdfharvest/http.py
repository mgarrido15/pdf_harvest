# src/pdfharvest/http.py
from __future__ import annotations
import asyncio
import logging
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Optional

import httpx  # library for making HTTP requests

CROSSREF = "https://api.crossref.org/works/"
UNPAYWALL = "https://api.unpaywall.org/v2/"

async def backoff_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Perform an HTTP request with retry and backoff for transient errors."""
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

async def fetch_crossref(client: httpx.AsyncClient, doi: str) -> Dict[str, Any]:
    """Fetch metadata from Crossref for a DOI."""
    url = CROSSREF + urllib.parse.quote(doi, safe="")
    r = await backoff_request(client, "GET", url, timeout=20)
    data = r.json()
    return data.get("message", {}) if isinstance(data, dict) else {}

async def fetch_unpaywall(client: httpx.AsyncClient, doi: str, email: str) -> Dict[str, Any]:
    """Fetch metadata from Unpaywall for a DOI."""
    url = UNPAYWALL + urllib.parse.quote(doi, safe="")
    r = await backoff_request(client, "GET", url, params={"email": email}, timeout=20)
    if r.status_code == 404:
        return {}
    try:
        return r.json()
    except ValueError:
        return {}

def best_pdf_url(ua: Dict[str, Any]) -> Optional[str]:
    """Extract best PDF URL from Unpaywall metadata."""
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

async def download_pdf(client: httpx.AsyncClient, url: str, out_path: Path) -> bool:
    """Download a PDF file from URL and save to disk."""
    log = logging.getLogger("pdfharvest.http")
    try:
        async with client.stream("GET", url, timeout=40) as r:
            if r.status_code >= 400:
                log.warning(f"Failed to download PDF: {r.status_code}")
                return False
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("wb") as f:
                async for chunk in r.aiter_bytes():
                    if chunk:
                        f.write(chunk)
        with out_path.open("rb") as f:
            if f.read(4) != b"%PDF":
                log.warning(f"Not a valid PDF: {url}")
                out_path.unlink(missing_ok=True)
                return False
        return True
    except (httpx.RequestError, OSError) as e:
        log.warning(f"PDF download failed {url}: {e}")
        return False
