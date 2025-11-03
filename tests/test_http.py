# tests/test_http.py
import pytest
import httpx
from pathlib import Path
from pdfharvest.http import best_pdf_url, download_pdf

def test_best_pdf_url_prefers_best_oa():
    ua = {
        "best_oa_location": {"url_for_pdf": "https://example.com/pdf1.pdf"},
        "oa_locations": [{"url_for_pdf": "https://example.com/pdf2.pdf"}],
    }
    assert best_pdf_url(ua) == "https://example.com/pdf1.pdf"

def test_best_pdf_url_fallback_to_oa():
    ua = {
        "best_oa_location": None,
        "oa_locations": [{"url_for_pdf": "https://example.com/pdf2.pdf"}],
    }
    assert best_pdf_url(ua) == "https://example.com/pdf2.pdf"

@pytest.mark.asyncio
async def test_download_pdf(tmp_path: Path):
    # Creamos un archivo "PDF falso" en un servidor local simulado
    # Pero aquí no tenemos red, así que simulamos el resultado
    out_path = tmp_path / "fake.pdf"
    async with httpx.AsyncClient() as client:
        # Como no hay conexión, el download fallará pero no debe lanzar excepción
        ok = await download_pdf(client, "https://nonexistent.example/fake.pdf", out_path)
        assert ok is False
