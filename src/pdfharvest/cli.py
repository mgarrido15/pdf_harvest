import typer
import asyncio
from pdfharvest.config import load_config
from pdfharvest.orchestrator import run_batch

app = typer.Typer(help="PDF Harvest CLI")

@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to YAML config file"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run without downloading PDFs"),
    batch_size: int = typer.Option(None, "--batch-size", help="Override batch size from config"),
):
    """Run the batch process from a YAML config file."""
    cfg = load_config(config_path)
    if batch_size:
        cfg.batch_size = batch_size

    asyncio.run(run_batch(cfg, dry_run=dry_run))

cli = app  # para que los tests encuentren 'cli'

if __name__ == "__main__":
    app()
