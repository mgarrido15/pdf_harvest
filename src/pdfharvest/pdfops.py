# src/pdfharvest/pdfops.py
from pathlib import Path
import shutil

def move_pdf_atomic(src: Path, dst: Path) -> None:
    """Move a PDF file atomically (replace if exists)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
