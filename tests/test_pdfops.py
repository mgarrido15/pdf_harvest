# tests/test_pdfops.py
from pathlib import Path
from pdfharvest.pdfops import move_pdf_atomic, search_pdf
from reportlab.pdfgen import canvas

def test_move_pdf_atomic(tmp_path: Path):

    src = tmp_path / "file.pdf"
    src.write_bytes(b"%PDF test")
    dest_dir = tmp_path / "dest"
    moved_path = move_pdf_atomic(src, dest_dir)
    assert moved_path.exists()
    assert moved_path.name == "file.pdf"
    assert not src.exists()
    new_src = tmp_path / "file.pdf"
    new_src.write_bytes(b"%PDF test2")
    moved2 = move_pdf_atomic(new_src, dest_dir)
    assert moved2.exists()
    assert moved2.name.startswith("file_")
    assert not new_src.exists()

def make_pdf(path: Path, text: str):
    
    c = canvas.Canvas(str(path))
    c.drawString(100, 750, text)
    c.save()

def test_search_pdf_finds_text(tmp_path: Path):

    pdf_path = tmp_path / "sample.pdf"
    make_pdf(pdf_path, "This document mentions AGH University and IDUB program.")

    needles = ["AGH University", "Excellence", "IDUB"]
    result = search_pdf(pdf_path, needles)

    assert result["found"] is True
    assert "agh university" in result["matches"]
    assert "idub" in result["matches"]
    assert isinstance(result["pages"], list)
    assert 1 in result["pages"]
