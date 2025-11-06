import typer
import asyncio
from pathlib import Path
from pdfharvest.config import load_config
from pdfharvest.orchestrator import run_batch
from pdfharvest.logging import setup_logging

cli = typer.Typer(help="PDFHarvest - Automatic paper downloader")

@cli.command()
def run(
    config: str = typer.Argument(..., help="Path to YAML config file"),
    dry_run: bool = typer.Option(False, help="Run without downloading PDFs"),
    batch_size: int = typer.Option(None, help="Override batch size from config file"),
):
    cfg = load_config(config)
    out_dir = Path(cfg.output_dir) if hasattr(cfg, "output_dir") else Path("output")
    setup_logging(cfg.__dict__ if hasattr(cfg, "__dict__") else {}, out_dir)

    if batch_size:
        cfg.batch_size = batch_size
        typer.echo(f"Overriding batch size to {batch_size}")
    asyncio.run(run_batch(cfg, dry_run=dry_run))

if __name__ == "__main__":
    cli()
