NAME of the project: pdfharvest

The STRUCTURE of this project is:

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
    │    └─ test_orchestrator.py
    └─ data/
        └─ sample.json