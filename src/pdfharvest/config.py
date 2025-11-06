
# src/pdfharvest/config.py
from dataclasses import dataclass, field
from pathlib import Path
import yaml
from typing import Dict, Any

@dataclass
class AppConfig:
    data_dir: Path
    email: str
    output_dir: Path = Path("./output")
    concurrency: int = 5
    batch_size: int = 5
    input_excel: Path = Path("doi_data_pite_2025.xlsx") 
    doi_column: str = "doi" 

    folders: Dict[str, str] = field(default_factory=lambda: {
        "downloads": "downloads",
        "found": "output_found",
        "notfound": "output_notfound"
    })
    cache: Dict[str, Any] = field(default_factory=lambda: {
        "enabled": True,
        "force_refresh": False
    })

def load_config(path: str | Path) -> AppConfig:
    """Load YAML config file."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return AppConfig(
        data_dir=Path(data.get("data_dir")),
        email=data["email"],
        concurrency=data.get("concurrency", 5),
    )



