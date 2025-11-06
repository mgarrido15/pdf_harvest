# tests/test_cli.py
import pytest
from typer.testing import CliRunner
from pdfharvest.cli import cli

runner = CliRunner()

def test_cli_run_dry_run(monkeypatch, tmp_path):
    """Test CLI execution with --dry-run flag."""

    config_path = tmp_path / "config.yaml"
    config_path.write_text("""
email: "test@example.com"
input_excel: "data.xlsx"
doi_column: "doi"
output_dir: "output"
batch_size: 5
concurrency: 2
""")

    called = {}
    def fake_run_batch(cfg, dry_run):
        called["called"] = True
        called["dry_run"] = dry_run
        return None

    monkeypatch.setattr("pdfharvest.orchestrator.run_batch", fake_run_batch)

    result = runner.invoke(cli, ["run", str(config_path), "--dry-run", "--batch-size", "10"])

    assert result.exit_code == 0
    assert "Overriding batch size to 10" in result.output
    assert called["called"] is True
    assert called["dry_run"] is True
