# tests/test_orchestrator.py
import pytest
from unittest.mock import AsyncMock, patch
from pdfharvest.orchestrator import run_batch
from pdfharvest.config import AppConfig

@pytest.mark.asyncio
async def test_run_batch_dry_run(monkeypatch, tmp_path):
    cfg = AppConfig(data_dir=tmp_path, email="test@example.com")

    # Simulamos las funciones HTTP
    mock_fetch_crossref = AsyncMock(return_value={"title": "Fake Paper"})
    mock_fetch_unpaywall = AsyncMock(return_value={"best_oa_location": {"url_for_pdf": "https://fake.pdf"}})
    mock_download_pdf = AsyncMock(return_value=True)

    with patch("pdfharvest.orchestrator.fetch_crossref", mock_fetch_crossref), \
         patch("pdfharvest.orchestrator.fetch_unpaywall", mock_fetch_unpaywall), \
         patch("pdfharvest.orchestrator.download_pdf", mock_download_pdf):
        
        await run_batch(cfg, dry_run=True)

    mock_fetch_crossref.assert_called()
    mock_fetch_unpaywall.assert_called()
    # En dry_run no debe descargar PDFs
    mock_download_pdf.assert_not_called()
