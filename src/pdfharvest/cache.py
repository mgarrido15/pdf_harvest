from pathlib import Path
import json, pathlib, re

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

def cache_path(base: pathlib.Path, ns: str, doi: str) -> pathlib.Path:
    return base / "cache" / ns / f"{sanitize_filename(doi)}.json"

def sanitize_filename(s: str) -> str:
    s = s.strip().replace("doi:", "").replace("DOI:", "")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)
