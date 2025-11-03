# src/pdfharvest/orchestrator.py
''' orchestaror cordinates everything. Fetch metadata and download PDFs'''
import asyncio
import httpx
from pdfharvest.http import fetch_crossref, fetch_unpaywall, best_pdf_url, download_pdf
from pdfharvest.config import AppConfig

async def run_batch(cfg: AppConfig, dry_run: bool = False) -> None:
    """Run a simple batch process to download PDFs from DOIs."""
    dois = [
        "10.1038/s41586-020-2649-2",  # example DOIs
        "10.1126/science.abc1234",
    ]
    ''' we use async because await that let us run multiples downloads at the same time'''
    async with httpx.AsyncClient() as client:
        for doi in dois:
            print(f"Processing {doi}...")
            meta = await fetch_crossref(client, doi)
            upw = await fetch_unpaywall(client, doi, cfg.email)
            pdf_url = best_pdf_url(upw)
            if pdf_url and not dry_run:
                out_path = cfg.data_dir / f"{doi.replace('/', '_')}.pdf"
                ok = await download_pdf(client, pdf_url, out_path)
                print("Downloaded" if ok else "Failed")
            else:
                print("No PDF found or dry-run mode")
