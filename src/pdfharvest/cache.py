# src/pdfharvest/cache.py
from pathlib import Path
import json

def cache_write(path: Path, data: dict) -> None:
    """Write JSON data to cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def cache_read(path: Path) -> dict:
    """Read JSON data from cache, return empty dict if not found."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
