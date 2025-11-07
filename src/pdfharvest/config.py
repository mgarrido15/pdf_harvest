from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List
import yaml

@dataclass
class AppConfig:
    data_dir: Path = Path("./data")
    email: str = ""
    output_dir: Path = Path("./output")
    input_excel: Path = Path("doi_data_pite_2025.xlsx")
    doi_column: str = "doi"
    strings: List[str] = field(default_factory=list)
    batch_size: int = 5
    concurrency: int = 5
    folders: Dict[str, str] = field(default_factory=lambda: {
        "downloads": "downloads",
        "found": "output_found",
        "notfound": "output_notfound",
    })
    cache: Dict[str, Any] = field(default_factory=lambda: {"enabled": True, "force_refresh": False})
    timeouts: Dict[str, Any] = field(default_factory=lambda: {"connect": 15, "read": 30})
    http: Dict[str, Any] = field(default_factory=lambda: {
        "user_agent": "doi-harvest/2.0 (mailto:someone@example.com)",
        "max_keepalive": 20,
        "max_connections": 20
    })
    write_after_each_batch: bool = True
    logging: Dict[str, Any] = field(default_factory=lambda: {
        "level": "INFO", "file": "harvest.log", "rotate_bytes": 10_485_760, "backup_count": 5
    })



"""
    Load application configuration from a YAML file and return an AppConfig instance.

    Args:
        path (str | Path)

    Returns:
        AppConfig: Fully initialized configuration object with default values
                   applied for missing fields.
"""
def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    cfg = AppConfig(
        data_dir=Path(data.get("data_dir", "./data")),
        email=data.get("email", ""),
        output_dir=Path(data.get("output_dir", "./output")),
        input_excel=Path(data.get("input_excel", data.get("doi_input", "doi_data_pite_2025.xlsx"))),
        doi_column=data.get("doi_column", "doi"),
        strings=data.get("strings", []) or [],
        batch_size=int(data.get("batch_size", 5)),
        concurrency=int(data.get("concurrency", 5)),
        folders=data.get("folders", cfg_default := AppConfig().folders),
        cache=data.get("cache", AppConfig().cache),
        timeouts=data.get("timeouts", AppConfig().timeouts),
        http=data.get("http", AppConfig().http),
        write_after_each_batch=bool(data.get("write_after_each_batch", True)),
        logging=data.get("logging", AppConfig().logging)
    )
    return cfg
