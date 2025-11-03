# tests/test_pdfops.py
from pathlib import Path
from pdfharvest.pdfops import move_pdf_atomic

def test_move_pdf_atomic(tmp_path: Path):
    src = tmp_path / "source.pdf"
    dst = tmp_path / "dest" / "file.pdf"
    src.write_bytes(b"%PDF test file")

    move_pdf_atomic(src, dst)

    assert dst.exists()
    assert not src.exists()
    assert dst.read_bytes().startswith(b"%PDF")
