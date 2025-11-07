from __future__ import annotations
import asyncio, logging
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd
import httpx

from pdfharvest.http import fetch_crossref, fetch_unpaywall, best_pdf_url, download_pdf
from pdfharvest.pdfops import search_pdf, move_pdf_atomic
from pdfharvest.cache import cache_path, cache_read, cache_write, sanitize_filename


"""
    Prepare and process a single DOI: fetch metadata, check open access, and download PDF.

    Args:
        doi (str)
        cfg (AppConfig)
        api_client (httpx.AsyncClient)
        pdf_client (httpx.AsyncClient)
        out_dir (Path)
        dry_run (bool, optional)

    Returns:
        dict: Processed record containing DOI metadata, file paths, and match results.
"""
async def prepare_one(
    doi: str,
    cfg: Any,
    api_client: httpx.AsyncClient,
    pdf_client: httpx.AsyncClient,
    out_dir: Path,
    dry_run: bool = False
) -> Dict[str, Any]:
    log = logging.getLogger("pdfharvest.orchestrator")
    downloads = out_dir / cfg.folders["downloads"] if hasattr(cfg, "folders") else out_dir / "downloads"
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

    title = "; ".join(meta.get("title", [])) if meta.get("title") else ""
    journal = "; ".join(meta.get("container-title", [])) if meta.get("container-title") else ""
    authors = "; ".join(
        f"{a.get('given','')} {a.get('family','')}".strip()
        for a in (meta.get("author", []) or [])
    )

    return {
        "doi": doi,
        "title": title,
        "journal": journal,
        "authors": authors,
        "year": (meta.get("issued", {}).get("date-parts", [[None]])[0][0]) if meta.get("issued") else None,
        "is_oa": oa.get("is_oa", None),
        "pdf_url": pdf_url or "",
        "pdf_temp_path": temp_pdf,
        "pdf_final_path": "",
        "match_found": False,
        "matched_strings": "",
        "match_pages": "",
    }


"""
    Process a list of PDF download tasks (a single batch): download, validate and move to final location.

    Args:
        tasks (list[dict])
        pdf_client (httpx.AsyncClient)
        out_dir (Path)
        concurrency (int, optional)
        validate_pdf (Callable[[Path], bool], optional)

    Returns:
        list[dict]: List of updated task dictionaries with download status and any error messages added under keys like 'download_ok' and 'download_error'.
"""

async def process_batch_pdfs(rows: List[Dict[str, Any]], cfg: Any, out_dir: Path):
    log = logging.getLogger("pdfharvest.orchestrator")
    needles = getattr(cfg, "strings", [])
    found_dir = out_dir / cfg.folders["found"] if hasattr(cfg, "folders") else out_dir / "output_found"
    notfound_dir = out_dir / cfg.folders["notfound"] if hasattr(cfg, "folders") else out_dir / "output_notfound"

    for r in rows:
        if not r.get("pdf_temp_path"):
            continue

        pdf_path = Path(r["pdf_temp_path"])
        result = search_pdf(pdf_path, needles)

        r["match_found"] = result["found"]
        r["matched_strings"] = ", ".join(result["matches"])
        r["match_pages"] = ", ".join(map(str, result["pages"]))

        dest_dir = found_dir if result["found"] else notfound_dir
        final_path = move_pdf_atomic(pdf_path, dest_dir)
        r["pdf_final_path"] = str(final_path)



"""
    Process all DOIs in batches asynchronously, coordinating downloads and metadata extraction.

    Args:
        cfg (AppConfig)
        dry_run (bool, optional)

    Returns:
        None: Results are written incrementally to output files.
"""
async def run_batch(cfg: Any, dry_run: bool = False):
    log = logging.getLogger("pdfharvest.orchestrator")
    out_dir = Path(cfg.output_dir) if hasattr(cfg, "output_dir") else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(cfg.input_excel)
    doi_col = getattr(cfg, "doi_column", "doi")
    if doi_col not in df.columns:
        raise ValueError(f"Excel must contain column '{doi_col}'")
    dois = [str(x).strip() for x in df[doi_col].dropna().tolist()]

    batch_size = getattr(cfg, "batch_size", 5)
    all_rows: List[Dict[str, Any]] = []

    limits = httpx.Limits(
        max_keepalive_connections=int(cfg.http.get("max_keepalive", 10)) if getattr(cfg, "http", None) else 10,
        max_connections=int(cfg.http.get("max_connections", 10)) if getattr(cfg, "http", None) else 10,
    )
    timeout = httpx.Timeout(float(cfg.timeouts.get("read", 20.0)), connect=float(cfg.timeouts.get("connect", 10.0)))

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as api_client, \
               httpx.AsyncClient(limits=limits, timeout=timeout) as pdf_client:

        for start in range(0, len(dois), batch_size):
            batch = dois[start:start + batch_size]
            log.info(f"Processing batch {start // batch_size + 1} with {len(batch)} DOIs")

            sem = asyncio.Semaphore(getattr(cfg, "concurrency", 5))

            async def prep_limited(doi):
                async with sem:
                    return await prepare_one(doi, cfg, api_client, pdf_client, out_dir, dry_run=dry_run)

            rows = await asyncio.gather(*[prep_limited(doi) for doi in batch])

            if not dry_run:
                await asyncio.to_thread(lambda: None) 
                await process_batch_pdfs(rows, cfg, out_dir)

            all_rows.extend(rows)

            if getattr(cfg, "write_after_each_batch", True):
                out_df = pd.DataFrame(all_rows)
                out_df.to_excel(out_dir / "report.xlsx", index=False)
                out_df.to_csv(out_dir / "report.csv", index=False, encoding="utf-8")
                log.info(f"Incremental report written: {len(out_df)} rows")

    out_df = pd.DataFrame(all_rows)
    out_df.to_excel(out_dir / "report.xlsx", index=False)
    out_df.to_csv(out_dir / "report.csv", index=False, encoding="utf-8")
    log.info(f"Done. Total DOIs processed: {len(all_rows)}")
    return all_rows
