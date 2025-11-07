from pathlib import Path
import json, pathlib, re


"""
    Write a JSON-serializable dictionary to the given cache path.

    Args:
        path (Path)
        data (dict)

    Returns:
        None

    Side Effects:
        Creates parent directories if they do not exist, and overwrites existing cache files.
"""
def cache_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


"""
    Read and parse a JSON cache file if it exists, returning a dictionary.

    Args:
        path (Path)

    Returns:
        dict: Parsed dictionary from the cache file, or an empty dict if the file
              does not exist or cannot be decoded.
"""
def cache_read(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


"""
    Construct the filesystem path for a cache entry given a base directory,
    a namespace, and a DOI identifier.

    Args:
        base (Path)
        ns (str)
        doi (str)

    Returns:
        Path: Full Path where the cache entry should be stored (not guaranteed to exist yet).
"""
def cache_path(base: pathlib.Path, ns: str, doi: str) -> pathlib.Path:
    return base / "cache" / ns / f"{sanitize_doi(doi)}.json"


"""
    Sanitize a doi to make it safe for url.

    Args:
        s (str)

    Returns:
        str: Sanitized filename-safe version of the input, with invalid characters replaced by underscores.
"""
def sanitize_doi(s: str) -> str:
    s = s.strip().replace("doi:", "").replace("DOI:", "")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)
