# tests/test_config.py
from pathlib import Path
import yaml
from pdfharvest.config import load_config, AppConfig

def test_load_config(tmp_path: Path):
   
    cfg_data = {
        "email": "test@example.com",
        "data_dir": str(tmp_path / "data"),
        "concurrency": 3
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(cfg_data), encoding="utf-8")

    cfg = load_config(cfg_path)

    assert isinstance(cfg, AppConfig)
    assert cfg.email == "test@example.com"
    assert cfg.data_dir.name == "data"
    assert cfg.concurrency == 3
