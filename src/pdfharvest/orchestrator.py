
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import httpx

from pdfharvest.http import fetch_crossref, fetch_unpaywall, best_pdf_url, download_pdf
from pdfharvest.pdfops import search_pdf, move_pdf_atomic
from pdfharvest.cache import cache_path, cache_read, cache_write, sanitize_filename


async def prepare_one(
    doi: str,
    cfg: Any,
    api_client: httpx.AsyncClient,
    pdf_client: httpx.AsyncClient,
    out_dir: Path,
    dry_run: bool = False
) -> Dict[str, Any]:

    log = logging.getLogger("pdfharvest.orchestrator")
    downloads = out_dir / cfg.folders["downloads"]
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
    ok = False 

    if pdf_url:
        out_path = downloads / f"{sanitize_filename(doi)}.pdf"
        if dry_run:
            log.info(f"[dry-run] Would download {doi} -> {out_path}")
        else:
            ok = await download_pdf(pdf_client, pdf_url, out_path)
            if ok:
                temp_pdf = str(out_path)


    title = "; ".join(meta.get("title", [])) if meta.get("title") else ""
    journal = "; ".join(meta.get("container-title", [])) if meta.get("container-title") else ""
    authors = "; ".join(
        f"{a.get('given', '')} {a.get('family', '')}".strip()
        for a in meta.get("author", [])
    )

    return {
        "doi": doi,
        "title": title,
        "journal": journal,
        "authors": authors,
        "year": meta.get("issued", {}).get("date-parts", [[None]])[0][0],
        "is_oa": oa.get("is_oa", None),
        "pdf_url": pdf_url or "",
        "pdf_temp_path": temp_pdf,
        "match_found": False,
        "matched_strings": "",
        "match_pages": "",
    }


async def process_batch_pdfs(rows: List[Dict[str, Any]], cfg: Any, out_dir: Path):

    log = logging.getLogger("pdfharvest.orchestrator")
    needles = cfg.strings
    found_dir = out_dir / cfg.folders["found"]
    notfound_dir = out_dir / cfg.folders["notfound"]

    for r in rows:
        pdf_temp = r.get("pdf_temp_path")
        if not pdf_temp or not Path(pdf_temp).exists():
            continue

        pdf_path = Path(pdf_temp)
        result = search_pdf(pdf_path, needles)

        r["match_found"] = result["found"]
        r["matched_strings"] = ", ".join(result["matches"])
        r["match_pages"] = ", ".join(map(str, result["pages"]))

        dest_dir = found_dir if result["found"] else notfound_dir
        final_path = move_pdf_atomic(pdf_path, dest_dir)
        r["pdf_final_path"] = str(final_path)


async def run_batch(cfg: Any, dry_run: bool = False):
    """Main orchestrator: loads DOIs, processes them in batches, and writes reports."""
    log = logging.getLogger("pdfharvest.orchestrator")
    out_dir = Path(cfg.output_dir) if getattr(cfg, "output_dir", None) else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

   
    dois = []
    if getattr(cfg, "input_excel", None) and Path(cfg.input_excel).exists():
        df = pd.read_excel(cfg.input_excel)
        dois = [str(x).strip() for x in df[cfg.doi_column].dropna().tolist()]
    else:
        
        dois = ["10.1234/fake-doi"]

    batch_size = getattr(cfg, "batch_size", 1)
    all_rows: List[Dict[str, Any]] = []

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    timeout = httpx.Timeout(20.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as api_client, \
               httpx.AsyncClient(limits=limits, timeout=timeout) as pdf_client:

        for start in range(0, len(dois), batch_size):
            batch = dois[start:start + batch_size]
            log.info(f"Processing batch {start // batch_size + 1} with {len(batch)} DOIs")

            sem = asyncio.Semaphore(getattr(cfg, "concurrency", 1))

            async def prep_limited(doi):
                async with sem:
                    return await prepare_one(doi, cfg, api_client, pdf_client, out_dir, dry_run=dry_run)

            rows = await asyncio.gather(*[prep_limited(doi) for doi in batch])

            
            if not dry_run:
                await process_batch_pdfs(rows, cfg, out_dir)

            all_rows.extend(rows)

            out_df = pd.DataFrame(all_rows)
            out_df.to_excel(out_dir / "report.xlsx", index=False)
            out_df.to_csv(out_dir / "report.csv", index=False, encoding="utf-8")

    log.info(f"Done. Total DOIs processed: {len(all_rows)}")
    return all_rows
