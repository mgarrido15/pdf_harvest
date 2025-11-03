# harvest_batched.py
# Two-stage (batched) DOI harvester:
#  1) For a batch of N DOIs, fetch metadata/OA and download PDFs into downloads/
#  2) Pause network work; process the batch's PDFs; move each to output_found/ or output_notfound/
# Usage (CLI):   python harvest_batched.py --config config.yaml
# Usage (Jupyter):   await harvest_batched.run("config.yaml")

import asyncio, json, logging, logging.handlers, pathlib, re, urllib.parse, argparse, shutil
from typing import Dict, Any, List, Optional

import httpx
import pandas as pd
import yaml
from pypdf import PdfReader
from tqdm.asyncio import tqdm_asyncio

CROSSREF = "https://api.crossref.org/works/"
UNPAYWALL = "https://api.unpaywall.org/v2/"

# ---------------- Logging ----------------

def setup_logging(cfg: Dict[str, Any], out_dir: pathlib.Path):
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    (out_dir / "logs").mkdir(parents=True, exist_ok=True)
    log_file = (out_dir / "logs" / log_cfg.get("file", "harvest.log")).resolve()
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=int(log_cfg.get("rotate_bytes", 10_485_760)),
        backupCount=int(log_cfg.get("backup_count", 5)), encoding="utf-8"
    )
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(level=level, format=fmt, handlers=[handler, logging.StreamHandler()])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("harvest")


# ---------------- Utils & dirs ----------------

def sanitize_filename(s: str) -> str:
    s = s.strip().replace("doi:", "").replace("DOI:", "")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)

def ensure_dirs(base: pathlib.Path, cfg: Dict[str, Any]):
    (base / "cache" / "crossref").mkdir(parents=True, exist_ok=True)
    (base / "cache" / "unpaywall").mkdir(parents=True, exist_ok=True)
    (base / "cache" / "matches").mkdir(parents=True, exist_ok=True)
    # staging + final folders
    (base / cfg["folders"]["downloads"]).mkdir(parents=True, exist_ok=True)
    (base / cfg["folders"]["found"]).mkdir(parents=True, exist_ok=True)
    (base / cfg["folders"]["notfound"]).mkdir(parents=True, exist_ok=True)

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# simple JSON cache (one file per DOI per namespace)
def cache_path(base: pathlib.Path, ns: str, doi: str) -> pathlib.Path:
    return base / "cache" / ns / f"{sanitize_filename(doi)}.json"

def read_cache_json(path: pathlib.Path) -> Optional[Dict[str, Any]]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def write_cache_json(path: pathlib.Path, data: Dict[str, Any]):
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logging.getLogger("harvest").warning(f"Cache write failed {path}: {e}")


# ---------------- HTTP helpers ----------------

async def backoff_request(client: httpx.AsyncClient, method: str, url: str, **kwargs) -> httpx.Response:
    log = logging.getLogger("harvest")
    max_tries, base = 6, 0.5
    for i in range(max_tries):
        try:
            r = await client.request(method, url, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra else min(base * (2**i), 10.0)
                log.warning(f"{r.status_code} {url} → backoff {wait:.2f}s (try {i+1}/{max_tries})")
                await asyncio.sleep(wait); continue
            r.raise_for_status()
            return r
        except httpx.HTTPError as e:
            if i == max_tries - 1:
                log.error(f"HTTP error {url}: {e}")
                raise
            await asyncio.sleep(min(base * (2**i), 10.0))
    raise RuntimeError("unreachable")

async def fetch_crossref(client: httpx.AsyncClient, doi: str) -> Dict[str, Any]:
    r = await backoff_request(client, "GET", CROSSREF + urllib.parse.quote(doi), timeout=20)
    return r.json().get("message", {})

async def fetch_unpaywall(client: httpx.AsyncClient, doi: str, email: str) -> Dict[str, Any]:
    r = await backoff_request(client, "GET", UNPAYWALL + urllib.parse.quote(doi),
                              params={"email": email}, timeout=20)
    if r.status_code == 404:
        return {}
    return r.json()

def best_pdf_url(ua: Dict[str, Any]) -> Optional[str]:
    if not ua: return None
    loc = ua.get("best_oa_location") or {}
    pdf = loc.get("url_for_pdf") or loc.get("url")
    if pdf: return pdf
    for loc in ua.get("oa_locations") or []:
        pdf = loc.get("url_for_pdf") or loc.get("url")
        if pdf: return pdf
    return None

async def download_pdf(client: httpx.AsyncClient, url: str, out_path: pathlib.Path) -> bool:
    log = logging.getLogger("harvest")
    try:
        async with client.stream("GET", url, timeout=40) as r:
            if r.status_code >= 400:
                log.warning(f"PDF {url} → {r.status_code}")
                return False
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                async for chunk in r.aiter_bytes():
                    f.write(chunk)
        with open(out_path, "rb") as f:
            if f.read(4) != b"%PDF":
                log.warning(f"Not a PDF (magic header) → {url}")
                out_path.unlink(missing_ok=True); return False
        return True
    except Exception as e:
        log.warning(f"PDF download failed {url}: {e}")
        return False


# ---------------- PDF processing & moves ----------------

def search_pdf(pdf_path: pathlib.Path, needles: List[str]) -> Dict[str, Any]:
    """
    Text search (casefolded substrings). If you need OCR later, add an opt-in pass here.
    """
    res = {"found": False, "matches": [], "pages": []}
    try:
        reader = PdfReader(str(pdf_path))
        ns = [n.casefold() for n in needles]
        hits, pages = set(), set()
        for i, p in enumerate(reader.pages):
            try:
                txt = (p.extract_text() or "").casefold()
            except Exception:
                txt = ""
            if not txt:
                continue
            page_hit = False
            for n in ns:
                if n in txt:
                    hits.add(n); page_hit = True
            if page_hit:
                pages.add(i + 1)
        if hits:
            res.update(found=True, matches=sorted(hits), pages=sorted(pages))
    except Exception as e:
        logging.getLogger("harvest").warning(f"PDF parse failed {pdf_path}: {e}")
    return res

def move_pdf_atomic(src: pathlib.Path, dst_dir: pathlib.Path) -> pathlib.Path:
    """
    Move a file atomically, preserving name; if collision, append a counter.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if not target.exists():
        return src.replace(target)
    stem, suf = src.stem, src.suffix
    k = 1
    while True:
        cand = dst_dir / f"{stem}_{k}{suf}"
        if not cand.exists():
            return src.replace(cand)
        k += 1


# ---------------- Per-DOI "prepare" (metadata + OA + download) ----------------

async def prepare_one(
    doi: str, cfg: Dict[str, Any], api_client: httpx.AsyncClient, pdf_client: httpx.AsyncClient,
    out_dir: pathlib.Path
) -> Dict[str, Any]:
    """
    Stage 1 for a DOI:
      - Load or fetch Crossref + Unpaywall
      - If OA PDF URL exists, download to downloads/ (staging folder)
      - Return a record with: metadata, OA status, temp pdf path (if any)
    """
    log = logging.getLogger("harvest")
    cache_en   = bool(cfg.get("cache", {}).get("enabled", True))
    force_ref  = bool(cfg.get("cache", {}).get("force_refresh", False))
    downloads  = out_dir / cfg["folders"]["downloads"]
    # cache files
    xref_cache = cache_path(out_dir, "crossref", doi)
    upw_cache  = cache_path(out_dir, "unpaywall", doi)

    # cached?
    meta = read_cache_json(xref_cache) if (cache_en and not force_ref) else None
    oa   = read_cache_json(upw_cache)  if (cache_en and not force_ref) else None

    if meta is None:
        try:
            meta = await fetch_crossref(api_client, doi)
            if cache_en: write_cache_json(xref_cache, meta)
        except Exception:
            meta = {}
    if oa is None:
        try:
            oa = await fetch_unpaywall(api_client, doi, cfg["email"])
            if cache_en: write_cache_json(upw_cache, oa)
        except Exception:
            oa = {}

    pdf_url = best_pdf_url(oa)
    temp_pdf = ""
    if pdf_url:
        # Always stage to downloads/ first
        fname = f"{sanitize_filename(doi)}.pdf"
        tgt = downloads / fname
        if tgt.exists() and not force_ref:
            temp_pdf = str(tgt)
        else:
            ok = await download_pdf(pdf_client, pdf_url, tgt)
            if ok:
                temp_pdf = str(tgt)

    # flatten some meta now
    title = "; ".join(meta.get("title", []) or [])
    journal = "; ".join(meta.get("container-title", []) or [])
    try:
        year = (meta.get("issued", {}).get("date-parts", [[None]])[0] or [None])[0]
    except Exception:
        year = None
    authors = "; ".join(f"{a.get('given','')} {a.get('family','')}".strip()
                        for a in (meta.get("author", []) or []))

    row = {
        "doi": doi, "title": title, "journal": journal, "year": year, "authors": authors,
        "publisher": meta.get("publisher",""), "type": meta.get("type",""),
        "crossref_url": meta.get("URL",""),
        "is_oa": oa.get("is_oa", None),
        "oa_license": (oa.get("best_oa_location") or {}).get("license", None),
        "pdf_url": pdf_url or "",
        "pdf_temp_path": temp_pdf,      # staged location (downloads/)
        "pdf_final_path": "",           # will be set in stage 2
        "match_found": False, "matched_strings": "", "match_pages": "",
    }
    log.debug(f"Prepared {doi} | OA={row['is_oa']} | temp_pdf={bool(temp_pdf)}")
    return row


# ---------------- Stage 2: process + route to found/notfound ----------------

async def process_batch_pdfs(rows: List[Dict[str, Any]], cfg: Dict[str, Any], out_dir: pathlib.Path):
    """
    For the batch's rows that have a staged PDF:
      - Search each PDF (thread executor)
      - Depending on hit, move the file to output_found/ or output_notfound/
      - Update rows in-place with match info & final path
      - Cache match results (so re-runs are fast)
    """
    log = logging.getLogger("harvest")
    needles = cfg.get("strings", [])
    cache_en   = bool(cfg.get("cache", {}).get("enabled", True))
    force_ref  = bool(cfg.get("cache", {}).get("force_refresh", False))

    found_dir    = out_dir / cfg["folders"]["found"]
    notfound_dir = out_dir / cfg["folders"]["notfound"]

    # Build tasks only for rows with a temp PDF and not cached match (unless force_refresh)
    to_process = []
    for r in rows:
        if not r.get("pdf_temp_path"):   # nothing to process
            continue
        m_cache = cache_path(out_dir, "matches", r["doi"])
        cached = read_cache_json(m_cache) if (cache_en and not force_ref) else None
        to_process.append((r, m_cache, cached))

    loop = asyncio.get_running_loop()
    # run PDF parsing concurrently in thread pool
    futs = []
    for r, m_cache, cached in to_process:
        if cached is not None:
            r["match_found"] = bool(cached.get("found"))
            r["matched_strings"] = ", ".join(cached.get("matches", []))
            r["match_pages"] = ", ".join(map(str, cached.get("pages", [])))
            continue
        futs.append(loop.run_in_executor(None, search_pdf, pathlib.Path(r["pdf_temp_path"]), needles))

    # collect fresh parsing results in the same order
    idx = 0
    for r, m_cache, cached in to_process:
        if cached is not None:
            # will move below based on cached result
            pass
        else:
            res = await futs[idx]; idx += 1
            r["match_found"] = bool(res.get("found"))
            r["matched_strings"] = ", ".join(res.get("matches", []))
            r["match_pages"] = ", ".join(map(str, res.get("pages", [])))
            if cache_en:
                write_cache_json(m_cache, res)

        # move the staged file according to match flag
        src = pathlib.Path(r["pdf_temp_path"])
        if not src.exists():
            continue  # might have been moved already on a previous run
        dest_dir = found_dir if r["match_found"] else notfound_dir
        final_path = move_pdf_atomic(src, dest_dir)
        r["pdf_final_path"] = str(final_path)
        # wipe temp path so re-runs won't try to move again
        r["pdf_temp_path"] = ""
        log.debug(f"Routed {r['doi']} → {'FOUND' if r['match_found'] else 'NOTFOUND'} | {final_path.name}")


# ---------------- Orchestrator ----------------

async def run(cfg_path: str):
    """
    Batch orchestrator:
      - Read config + Excel DOIs
      - For DOIs in chunks of batch_size:
          * Stage 1: concurrently prepare (metadata+OA) and download PDFs into downloads/
          * Stage 2: pause downloading; process batch PDFs and move to final folders
      - Append results and write report.xlsx/.csv at the end (and optionally after each batch)
    """
    cfg = load_yaml(cfg_path)
    out_dir = pathlib.Path(cfg.get("output_dir", "output")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_dirs(out_dir, cfg)

    log = setup_logging(cfg, out_dir)
    log.info("Starting batched DOI harvest")

    # input
    df = pd.read_excel(cfg["input_excel"])
    doi_col = cfg.get("doi_column", "doi")
    if doi_col not in df.columns:
        raise ValueError(f"Excel must contain column '{doi_col}'")
    dois = [str(x).strip() for x in df[doi_col].dropna().tolist()]
    log.info(f"Loaded {len(dois)} DOIs")

    # HTTP clients (kept open across batches)
    headers = {"User-Agent": cfg.get("http", {}).get("user_agent", f"doi-harvest/2.0 (+{cfg.get('email','')})")}
    limits = httpx.Limits(
        max_keepalive_connections=int(cfg.get("http", {}).get("max_keepalive", 20)),
        max_connections=int(cfg.get("http", {}).get("max_connections", 20)),
    )
    timeout = httpx.Timeout(
        float(cfg.get("timeouts", {}).get("read", 30.0)),
        connect=float(cfg.get("timeouts", {}).get("connect", 15.0))
    )
    batch_size = int(cfg.get("batch_size", 5))
    # polite parallelism *within a batch* (metadata+downloads)
    per_batch_concurrency = int(cfg.get("concurrency", min(batch_size, 6)))

    all_rows: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(headers=headers, limits=limits, timeout=timeout, http2=True) as api_client, \
               httpx.AsyncClient(headers=headers, limits=limits, timeout=timeout, http2=True) as pdf_client:

        for start in range(0, len(dois), batch_size):
            chunk = dois[start:start + batch_size]
            log.info(f"Batch {start//batch_size + 1}: preparing {len(chunk)} DOIs")
            sem = asyncio.Semaphore(per_batch_concurrency)

            # ------ Stage 1: prepare+download (bounded concurrency), staged into downloads/ ------
            async def prep_wrapped(doi):
                async with sem:
                    return await prepare_one(doi, cfg, api_client, pdf_client, out_dir)

            prep_tasks = [prep_wrapped(doi) for doi in chunk]
            rows = await tqdm_asyncio.gather(*prep_tasks, total=len(prep_tasks), desc="Stage 1: prepare+download")

            # ------ Stage 2: processing (no network; only CPU and file moves) ------
            log.info(f"Batch {start//batch_size + 1}: processing PDFs")
            await process_batch_pdfs(rows, cfg, out_dir)

            all_rows.extend(rows)

            # optional: write incremental report after each batch
            if cfg.get("write_after_each_batch", True):
                out_df = pd.DataFrame(all_rows)
                out_df.to_excel(out_dir / "report.xlsx", index=False)
                out_df.to_csv(out_dir / "report.csv", index=False, encoding="utf-8")
                log.info(f"Incremental report written: {len(out_df)} rows")

    # final report
    out_df = pd.DataFrame(all_rows)
    out_df.to_excel(out_dir / "report.xlsx", index=False)
    out_df.to_csv(out_dir / "report.csv", index=False, encoding="utf-8")
    log.info(f"Done. Total rows: {len(out_df)} → {out_dir/'report.xlsx'}")
    return out_df


# CLI shim
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Batched DOI harvester: staged downloads → processing → routed outputs")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    args = ap.parse_args()
    asyncio.run(run(args.config))

