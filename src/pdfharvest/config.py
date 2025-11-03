
# src/pdfharvest/config.py
from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass
class AppConfig:
    data_dir: Path
    email: str
    concurrency: int = 5

def load_config(path: str | Path) -> AppConfig:
    """Load YAML config file."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return AppConfig(
        data_dir=Path(data.get("data_dir", "./data")),
        email=data["email"],
        concurrency=data.get("concurrency", 5),
    )



'''
email: "laurasancho024@gmail.com"
input_excel: "data/doi1.xlsx"
doi_column: "doi"

# What to search for in PDFs (case-insensitive)
strings:
  - "Excellence Initiative – Research University"
  - "IDUB"
  - "AGH University"

output_dir: "output"

# NEW: batch and intra-batch parallelism
batch_size: 5             # download up to 5 PDFs, then pause and process them
concurrency: 5            # max concurrent DOI prep inside a batch

# Folders (relative to output_dir)
folders:
  downloads: "downloads"        # staging folder for freshly downloaded PDFs
  found: "output_found"         # final destination if a match is found
  notfound: "output_notfound"   # final destination if no match

# Caching and reporting
cache:
  enabled: true
  force_refresh: false
write_after_each_batch: true

# Networking
timeouts:
  connect: 15
  read: 30
http:
  user_agent: "doi-harvest/2.0 (+szumlak@agh.edu.edu)"
  max_keepalive: 20
  max_connections: 20

  '''