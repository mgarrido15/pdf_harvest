# src/pdfharvest/cli.py
import typer
import asyncio
from pdfharvest.config import load_config
from pdfharvest.orchestrator import run_batch
from pdfharvest.logging import setup_logging

cli = typer.Typer(help="PDFHarvest - Automatic paper downloader")

@cli.command()
def run(config: str, dry_run: bool = typer.Option(False, help="Run without downloading PDFs")):
    """Run the batch process from a YAML config file."""
    setup_logging()
    cfg = load_config(config)
    asyncio.run(run_batch(cfg, dry_run=dry_run))

if __name__ == "__main__":
    cli()
