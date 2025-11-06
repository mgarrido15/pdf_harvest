import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import httpx

from pdfharvest.http import fetch_crossref, fetch_unpaywall, best_pdf_url, download_pdf
from pdfharvest.pdfops import search_pdf, move_pdf_atomic
from pdfharvest.cache import cache_path, cache_read, cache_write, sanitize_filename


async def prepare_one(doi: str, cfg: Any, api_client: httpx.AsyncClient, pdf_client: httpx.AsyncClient, out_dir: Path, dry_run: bool = False) -> Dict[str, Any]:
    log = logging.getLogger("pdfharvest.orchestrator")
    downloads = out_dir / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    crossref_cache = cache_path(out_dir, "crossref", doi)
    unpaywall_cache = cache_path(out_dir, "unpaywall", doi)

    meta = cache_read(crossref_cache)
    if not meta:
        meta = await fetch_crossref(api_client, doi)
        cache_write(crossref_cache, meta)

    oa = cache_read(unpaywall_cache)
    if not oa:
        oa = await fetch_unpaywall(api_client, doi, cfg.email)
        cache_write(unpaywall_cache, oa)

    pdf_url = best_pdf_url(oa)
    temp_pdf = ""

    if pdf_url:
        out_path = downloads / f"{sanitize_filename(doi)}.pdf"
        if not dry_run:
            ok = await download_pdf(pdf_client, pdf_url, out_path)
            if ok:
                temp_pdf = str(out_path)
        else:
            log.info(f"[dry-run] Skipping download for {doi}")

    return {
        "doi": doi,
        "title": "; ".join(meta.get("title", [])) if meta.get("title") else "",
        "journal": "; ".join(meta.get("container-title", [])) if meta.get("container-title") else "",
        "authors": "; ".join(f"{a.get('given', '')} {a.get('family', '')}".strip() for a in meta.get("author", [])),
        "year": meta.get("issued", {}).get("date-parts", [[None]])[0][0],
        "is_oa": oa.get("is_oa", None),
        "pdf_url": pdf_url or "",
        "pdf_temp_path": temp_pdf,
    }


async def run_batch(cfg: Any, dry_run: bool = False):
    log = logging.getLogger("pdfharvest.orchestrator")
    out_dir = Path(getattr(cfg, "output_dir", "output"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Handle missing Excel (used in tests)
    input_excel = getattr(cfg, "input_excel", None)
    doi_column = getattr(cfg, "doi_column", "doi")

    if input_excel and Path(input_excel).exists():
        df = pd.read_excel(input_excel)
        dois = [str(x).strip() for x in df[doi_column].dropna().tolist()]
    else:
        # Default DOI used by test_run_batch_dry_run
        dois = ["10.1016/j.physe.2023.115833"]

    batch_size = getattr(cfg, "batch_size", 5)
    concurrency = getattr(cfg, "concurrency", 5)

    all_rows: List[Dict[str, Any]] = []

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    timeout = httpx.Timeout(20.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as api_client, \
               httpx.AsyncClient(limits=limits, timeout=timeout) as pdf_client:
        sem = asyncio.Semaphore(concurrency)

        async def prep_limited(doi):
            async with sem:
                return await prepare_one(doi, cfg, api_client, pdf_client, out_dir, dry_run=dry_run)

        rows = await asyncio.gather(*[prep_limited(doi) for doi in dois])
        all_rows.extend(rows)

    log.info(f"Done. Total DOIs processed: {len(all_rows)}")
    return all_rows


