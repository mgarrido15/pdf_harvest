import click
from pdfharvest.config import load_config
from pdfharvest.orchestrator import run_batch
import asyncio


@click.group()
def cli():
   
    pass


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Run without downloading or writing files")
@click.option("--batch-size", type=int, help="Override batch size from config")
def run(config_path, dry_run, batch_size):
   
    cfg = load_config(config_path)
    if batch_size:
        cfg.batch_size = batch_size

    asyncio.run(run_batch(cfg, dry_run=dry_run))
