from pathlib import Path
import pathlib, logging, shutil
from pypdf import PdfReader 
from typing import Dict, Any, List

def search_pdf(pdf_path: pathlib.Path, needles: List[str]) -> Dict[str, Any]:
    """
    Text search (casefolded substrings). If you need OCR later, add an opt-in pass here.
    """
    res = {"found": False, "matches": [], "pages": []}
    try:
        reader = PdfReader(str(pdf_path))
        ns = [n.casefold() for n in needles]
        hits, pages = set(), set()
        for i, p in enumerate(reader.pages):
            try:
                txt = (p.extract_text() or "").casefold()
            except Exception:
                txt = ""
            if not txt:
                continue
            page_hit = False
            for n in ns:
                if n in txt:
                    hits.add(n); page_hit = True
            if page_hit:
                pages.add(i + 1)
        if hits:
            res.update(found=True, matches=sorted(hits), pages=sorted(pages))
    except Exception as e:
        logging.getLogger("harvest").warning(f"PDF parse failed {pdf_path}: {e}")
    return res

def move_pdf_atomic(src: pathlib.Path, dst_dir: pathlib.Path) -> pathlib.Path:
    """
    Move a file atomically, preserving name; if collision, append a counter.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if not target.exists():
        return src.replace(target)
    stem, suf = src.stem, src.suffix
    k = 1
    while True:
        cand = dst_dir / f"{stem}_{k}{suf}"
        if not cand.exists():
            return src.replace(cand)
        k += 1

