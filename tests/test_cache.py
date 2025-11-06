from pathlib import Path
import json
from pdfharvest.cache import sanitize_filename, cache_write, cache_read, cache_path


def test_sanitize_filename_basic():
    assert sanitize_filename("10.1038/s41586-020-2649-2") == "10.1038_s41586-020-2649-2"
    assert sanitize_filename("doi: 10 2000 abc") == "_10_2000_abc"
    assert sanitize_filename("A!@#B$%^C&*()") == "A_B_C_"
    assert sanitize_filename("simple") == "simple"


def test_cache_write_and_read(tmp_path: Path):
    """Check that data is written and read correctly from cache JSON."""
    file_path = tmp_path / "cache" / "crossref" / "test.json"
    data = {"title": "Array programming with NumPy", "year": 2020}

    cache_write(file_path, data)
    assert file_path.exists(), "El archivo no se creó correctamente"

    content = json.loads(file_path.read_text(encoding="utf-8"))
    assert content == data

    read_data = cache_read(file_path)
    assert read_data == data


def test_cache_read_nonexistent_file(tmp_path: Path):
    """If file doesn't exist, cache_read() should return empty dict."""
    file_path = tmp_path / "no_file.json"
    result = cache_read(file_path)
    assert result == {}, "Debe devolver un diccionario vacío si el archivo no existe"


def test_cache_read_invalid_json(tmp_path: Path):
    """If JSON is invalid, cache_read() should return empty dict."""
    file_path = tmp_path / "invalid.json"
    file_path.write_text("{ invalid json }", encoding="utf-8")

    result = cache_read(file_path)
    assert result == {}, "Debe devolver dict vacío si el JSON está mal formado"


def test_cache_path_builds_correct_path(tmp_path: Path):
    """Ensure cache_path creates proper safe path structure."""
    doi = "10.1038/s41586-020-2649-2"
    path = cache_path(tmp_path, "unpaywall", doi)
    expected = tmp_path / "cache" / "unpaywall" / "10.1038_s41586-020-2649-2.json"

    assert path == expected
    assert path.parent.name == "unpaywall"
    assert path.suffix == ".json"
