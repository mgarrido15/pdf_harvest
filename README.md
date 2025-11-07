# PDF Harvest

Automated tool designed to collect metadata and retrieve open-access versions of scientific articles using their DOI.  
It leverages the Crossref and Unpaywall APIs to gather bibliographic information and locate legally accessible PDF files.

## Structure

    pdfharvest/
    ├─ pyproject.toml
    ├─ README.md
    ├─ src/
    │  └─ pdfharvest/
    │     ├─ __init__.py
    │     ├─ cli.py
    │     ├─ config.py
          ├─ cache.py
          ├─ http.py
          ├─ pdfops.py
          ├─ orchestrator.py
    │     └─ logging.py
    ├─ tests/
    │    ├─ test_pdfops.py
    │    ├─ test_config.py
         ├─ test_http.py
         ├─ test_cache.py
         └─ test_orchestrator.py

## Requirements

- Python 3.10+  
- Internet connection (for Crossref / Unpaywall APIs)

## Authors

- Laura Sancho
- Marco Garrido

## Installation

Clone the repository:

```bash
git clone https://github.com/mgarrido15/pdf_harvest.git






