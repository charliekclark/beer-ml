#!/usr/bin/env python3
"""Export Brewers Friend recipe data via web scraping or the private API.

Modes:
  web      – (default) scrape public recipe pages discovered via sitemaps.
  api      – use the authenticated /v1/recipes endpoint (own recipes only).
  sitemap  – discover IDs from sitemaps, then hit /v1/recipes/{id} API.

The "web" mode writes one JSON object per line to a .jsonl output file,
supports concurrent workers, and can resume from a previous partial run.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from dotenv import dotenv_values, load_dotenv

BASE_URL = "https://api.brewersfriend.com/v1"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept-Language": "en-US,en;q=0.9",
}
DEFAULT_RECIPE_SITEMAPS = [
    f"https://www.brewersfriend.com/homebrew/sitemap-recipes-{i}.xml"
    for i in range(1, 8)
]


# ---------------------------------------------------------------------------
# .env helpers
# ---------------------------------------------------------------------------

def build_env_candidates(workspace: Path) -> list[Path]:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        workspace / ".env",
        workspace.parent / ".env",
        script_dir / ".env",
        script_dir.parent / ".env",
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for item in candidates:
        key = str(item.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def load_env_files(workspace: Path) -> None:
    for env_path in build_env_candidates(workspace):
        load_dotenv(env_path, override=False)


def resolve_api_key(workspace: Path) -> str | None:
    env_key = (os.getenv("bf_api_key") or os.getenv("BF_API_KEY") or "").strip()
    if env_key:
        return env_key
    for env_path in build_env_candidates(workspace):
        if not env_path.exists():
            continue
        values = dotenv_values(env_path)
        file_key = (values.get("bf_api_key") or values.get("BF_API_KEY") or "").strip()
        if file_key:
            return file_key
    return None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch(url: str, extra_headers: dict[str, str] | None = None, timeout: int = 60) -> bytes:
    hdrs = {**DEFAULT_HEADERS}
    if extra_headers:
        hdrs.update(extra_headers)
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def request_json(api_key: str, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
    qs = f"?{urllib.parse.urlencode(query)}" if query else ""
    url = f"{BASE_URL}{path}{qs}"
    try:
        body = _fetch(url, {"X-API-KEY": api_key, "Accept": "application/json"})
        return json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc}") from exc


def fetch_html(url: str, max_retries: int = 4) -> str:
    """Fetch a page and return decoded HTML with retry/backoff for 403 and 5xx."""
    extra = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.brewersfriend.com/",
    }
    for attempt in range(max_retries):
        try:
            raw = _fetch(url, extra, timeout=30)
            return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (403, 429, 500, 502, 503, 504)
            if retryable and attempt < max_retries - 1:
                backoff = 2 ** attempt + (attempt * 0.5)
                time.sleep(backoff)
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    return ""


# ---------------------------------------------------------------------------
# Sitemap discovery
# ---------------------------------------------------------------------------

def parse_xml_locs(xml_text: str) -> list[str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    return [
        el.text.strip()
        for el in root.iter()
        if el.tag.endswith("loc") and el.text
    ]


def extract_recipe_id(recipe_url: str) -> str | None:
    m = re.search(r"/recipe/view/(\d+)", recipe_url)
    return m.group(1) if m else None


def collect_sitemap_urls(sitemap_urls: list[str]) -> list[str]:
    """Download sitemaps and return sorted, deduplicated recipe page URLs."""
    all_urls: list[str] = []
    for sitemap_url in sitemap_urls:
        try:
            raw = _fetch(sitemap_url, {"Accept": "application/xml,text/xml,*/*"})
            locs = parse_xml_locs(raw.decode("utf-8", errors="replace"))
            recipe_locs = [u for u in locs if "/recipe/view/" in u]
            all_urls.extend(recipe_locs)
            print(f"  {sitemap_url}: {len(recipe_locs)} recipes", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING {sitemap_url}: {exc}", flush=True)
    deduped = sorted(set(all_urls))
    print(f"Total unique recipe URLs: {len(deduped)}", flush=True)
    return deduped


# ---------------------------------------------------------------------------
# Recipe text-block parser (embedded in every public recipe page)
# ---------------------------------------------------------------------------

_HTML_ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;|&#x[0-9a-fA-F]+;")


def _unescape(text: str) -> str:
    return _HTML_ENTITY_RE.sub(lambda m: html.unescape(m.group()), text)


def parse_recipe_text_block(text: str) -> dict[str, Any]:
    """Parse the 'HOME BREW RECIPE:' plain-text block into structured data."""
    result: dict[str, Any] = {}
    lines = [_unescape(ln.strip()) for ln in text.strip().splitlines()]

    section = "header"
    list_key = ""
    kv_target_key = ""
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line:
            continue

        # Section transitions
        if line == "STATS:":
            section = "stats"; continue
        if line == "FERMENTABLES:":
            section, list_key = "list", "fermentables"
            result["fermentables"] = []; continue
        if line == "HOPS:":
            section, list_key = "list", "hops"
            result["hops"] = []; continue
        if line == "OTHER INGREDIENTS:":
            section, list_key = "list", "other_ingredients"
            result["other_ingredients"] = []; continue
        if line == "YEAST:":
            section = "yeast"
            result["yeast"] = {}; continue
        if line == "MASH GUIDELINES:":
            section, list_key = "list", "mash_steps"
            result["mash_steps"] = []; continue
        if line == "TARGET WATER PROFILE:":
            section, kv_target_key = "kv", "water_profile"
            result["water_profile"] = {}; continue
        if line == "PRIMING:":
            section, kv_target_key = "kv", "priming"
            result["priming"] = {}; continue
        if line.startswith("WATER REQUIREMENTS:"):
            section = "skip"; continue
        if line.startswith("This recipe has been published"):
            section = "skip"; continue
        if line.startswith("Generated by"):
            break

        # Key-value sections
        if section in ("header", "stats"):
            kv = line.split(":", 1)
            if len(kv) == 2:
                key = kv[0].strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
                val = kv[1].strip()
                if key != "home_brew_recipe":
                    result[key] = val
            continue

        if section == "kv":
            kv = line.split(":", 1)
            if len(kv) == 2 and kv_target_key in result:
                key = kv[0].strip().lower().replace(" ", "_")
                result[kv_target_key][key] = kv[1].strip()
            continue

        if section == "skip":
            continue

        # List sections
        if section == "list" and list_key == "fermentables":
            m = re.match(r"^([\d.]+\s+\w+)\s*-\s*(.+?)\s*\(([\d.]+)%\)\s*$", line)
            if m:
                result["fermentables"].append({
                    "amount": m.group(1), "name": m.group(2), "bill_pct": float(m.group(3)),
                })
            continue

        if section == "list" and list_key == "hops":
            m = re.match(
                r"^([\d.]+\s+\w+)\s*-\s*(.+?)(?:,\s*Type:\s*(.+?))?(?:,\s*AA:\s*([\d.]+))?"
                r"(?:,\s*Use:\s*(.+?))?(?:,\s*IBU:\s*([\d.]+))?\s*$",
                line,
            )
            if m:
                result["hops"].append({
                    "amount": m.group(1), "name": m.group(2),
                    "type": (m.group(3) or "").strip(),
                    "alpha_acid": (m.group(4) or "").strip(),
                    "use": (m.group(5) or "").strip(),
                    "ibu": (m.group(6) or "").strip(),
                })
            continue

        if section == "list" and list_key == "other_ingredients":
            m = re.match(
                r"^(.+?)\s*-\s*(.+?)(?:,\s*Time:\s*(.+?))?(?:,\s*Type:\s*(.+?))?(?:,\s*Use:\s*(.+?))?\s*$",
                line,
            )
            if m:
                result["other_ingredients"].append({
                    "amount": m.group(1).strip(), "name": m.group(2).strip(),
                    "time": (m.group(3) or "").strip(),
                    "type": (m.group(4) or "").strip(),
                    "use": (m.group(5) or "").strip(),
                })
            continue

        if section == "list" and list_key == "mash_steps":
            result["mash_steps"].append(line)
            continue

        if section == "yeast":
            kv = line.split(":", 1)
            if len(kv) == 2:
                key = kv[0].strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
                result["yeast"][key] = kv[1].strip()
            elif "name" not in result["yeast"]:
                result["yeast"]["name"] = line
            else:
                result["yeast"].setdefault("additional_yeast", []).append(line)

    return result


# ---------------------------------------------------------------------------
# Web scraper – one recipe
# ---------------------------------------------------------------------------

_TEXT_BLOCK_RE = re.compile(r"HOME BREW RECIPE:(.*?)Generated by Brewer", re.DOTALL)


def scrape_one_recipe(url: str) -> dict[str, Any]:
    """Fetch a recipe page and return parsed data dict."""
    recipe_id = extract_recipe_id(url) or ""
    try:
        page_html = fetch_html(url, max_retries=2)
    except Exception as exc:  # noqa: BLE001
        return {"id": recipe_id, "url": url, "error": f"fetch: {exc}"}

    match = _TEXT_BLOCK_RE.search(page_html)
    if not match:
        return {"id": recipe_id, "url": url, "error": "no text block found"}

    parsed = parse_recipe_text_block(match.group(0))
    parsed["id"] = recipe_id
    parsed["url"] = url
    return parsed


# ---------------------------------------------------------------------------
# Web scraping pipeline
# ---------------------------------------------------------------------------

def load_done_ids(jsonl_path: Path) -> set[str]:
    """Read already-scraped recipe IDs from an existing JSONL file."""
    done: set[str] = set()
    if not jsonl_path.exists():
        return done
    with jsonl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rid = str(obj.get("id", ""))
                if rid:
                    done.add(rid)
            except json.JSONDecodeError:
                pass
    return done


def run_web_scrape(
    sitemap_urls: list[str],
    output_path: Path,
    max_recipes: int = 0,
    workers: int = 3,
    delay: float = 0.4,
) -> int:
    print("Phase 1: collecting recipe URLs from sitemaps...", flush=True)
    recipe_urls = collect_sitemap_urls(sitemap_urls)

    if max_recipes > 0:
        recipe_urls = recipe_urls[:max_recipes]

    done_ids = load_done_ids(output_path)
    remaining = [u for u in recipe_urls if extract_recipe_id(u) not in done_ids]
    print(f"Already scraped: {len(done_ids)}, remaining: {len(remaining)}", flush=True)

    if not remaining:
        print("Nothing to scrape.", flush=True)
        return len(done_ids)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    scraped = len(done_ids)
    total = len(done_ids) + len(remaining)
    errors = 0
    consecutive_errors = 0
    t0 = time.monotonic()

    with output_path.open("a", encoding="utf-8") as fh, \
         concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:

        future_to_url: dict[concurrent.futures.Future[dict[str, Any]], str] = {}
        batch_size = workers * 2

        for batch_start in range(0, len(remaining), batch_size):
            batch = remaining[batch_start : batch_start + batch_size]
            future_to_url.clear()

            for url in batch:
                fut = pool.submit(scrape_one_recipe, url)
                future_to_url[fut] = url
                time.sleep(delay)

            for fut in concurrent.futures.as_completed(future_to_url):
                result = fut.result()
                fh.write(json.dumps(result, ensure_ascii=True) + "\n")
                fh.flush()
                scraped += 1

                if "error" in result:
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= 10:
                        print(f"  Throttling: {consecutive_errors} consecutive errors, pausing 10s...", flush=True)
                        time.sleep(10)
                        consecutive_errors = 0
                else:
                    consecutive_errors = 0

                if scraped % 100 == 0:
                    elapsed = time.monotonic() - t0
                    rate = (scraped - len(done_ids)) / elapsed if elapsed > 0 else 0
                    eta_s = (total - scraped) / rate if rate > 0 else 0
                    eta_m = eta_s / 60
                    print(
                        f"  [{scraped}/{total}] {rate:.1f} recipes/s, "
                        f"errors={errors}, ETA {eta_m:.0f}m",
                        flush=True,
                    )

    elapsed = time.monotonic() - t0
    print(
        f"\nDone. Scraped {scraped - len(done_ids)} new recipes in {elapsed:.0f}s "
        f"({errors} errors). Total in file: {scraped}",
        flush=True,
    )
    return scraped


# ---------------------------------------------------------------------------
# API-based helpers (for own recipes)
# ---------------------------------------------------------------------------

def fetch_all_recipes(api_key: str, page_size: int = 100) -> list[dict[str, Any]]:
    all_recipes: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = request_json(api_key, "/recipes", query={"limit": page_size, "offset": offset})
        recipes = payload.get("recipes") or []
        if not recipes:
            break
        all_recipes.extend(recipes)
        print(f"Fetched {len(recipes)} recipes (offset {offset})", flush=True)
        if len(recipes) < page_size:
            break
        offset += page_size
        time.sleep(0.1)
    return all_recipes


def fetch_all_brew_sessions(api_key: str, page_size: int = 100) -> list[dict[str, Any]]:
    all_sessions: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = request_json(api_key, "/brewsessions", query={"limit": page_size, "offset": offset})
        sessions = payload.get("brewsessions") or []
        if not sessions:
            break
        all_sessions.extend(sessions)
        print(f"Fetched {len(sessions)} brew sessions (offset {offset})", flush=True)
        if len(sessions) < page_size:
            break
        offset += page_size
        time.sleep(0.1)
    return all_sessions


def first_item(payload: dict[str, Any], key: str) -> dict[str, Any] | None:
    values = payload.get(key)
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, dict):
            return first
    return None


def safe_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=True), encoding="utf-8")


def safe_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run_api_export(api_key: str, out_dir: Path, max_recipes: int) -> int:
    """Original API-based export flow (own recipes only)."""
    recipes = fetch_all_recipes(api_key, page_size=100)
    if not recipes:
        print("API returned no recipes (you may not have any saved recipes).", flush=True)
        return 0

    if max_recipes > 0:
        recipes = recipes[:max_recipes]

    sessions = fetch_all_brew_sessions(api_key, page_size=100)
    sessions_by_recipe: dict[str, list[dict[str, Any]]] = {}
    for session in sessions:
        rid = str(session.get("recipeid") or "").strip()
        if rid:
            sessions_by_recipe.setdefault(rid, []).append(session)

    safe_write_json(out_dir / "all_recipes_minimal.json", recipes)
    safe_write_json(out_dir / "all_brewsessions_minimal.json", sessions)

    full_export: list[dict[str, Any]] = []
    for idx, recipe in enumerate(recipes, 1):
        recipe_id = str(recipe.get("id"))
        print(f"[{idx}/{len(recipes)}] Enriching recipe {recipe_id}", flush=True)

        enriched: dict[str, Any] = {
            "recipe_summary": recipe, "recipe_detail": None,
            "recipe_photos": None, "recipe_beerxml_path": None,
            "brew_sessions_summary": sessions_by_recipe.get(recipe_id, []),
            "brew_sessions_detail": [], "brew_sessions_logs": [], "errors": [],
        }

        for label, path_tpl, parser in [
            ("recipe_detail", "/recipes/{rid}", lambda p: first_item(p, "recipes") or p),
            ("recipe_photos", "/recipes/{rid}/photos", lambda p: p),
        ]:
            try:
                payload = request_json(api_key, path_tpl.format(rid=recipe_id))
                enriched[label] = parser(payload)
            except Exception as exc:  # noqa: BLE001
                enriched["errors"].append(f"{label}: {exc}")

        try:
            from urllib.request import Request, urlopen  # noqa: F811
            xml_url = f"{BASE_URL}/recipes/{recipe_id}.xml"
            xml_req = Request(xml_url, headers={**DEFAULT_HEADERS, "X-API-KEY": api_key, "Accept": "*/*"})
            with urlopen(xml_req, timeout=60) as r:
                xml_text = r.read().decode("utf-8", errors="replace")
            xml_path = out_dir / "recipes" / recipe_id / "recipe.xml"
            safe_write_text(xml_path, xml_text)
            enriched["recipe_beerxml_path"] = str(xml_path.as_posix())
        except Exception as exc:  # noqa: BLE001
            enriched["errors"].append(f"recipe_beerxml: {exc}")

        for session in sessions_by_recipe.get(recipe_id, []):
            sid = str(session.get("id"))
            if not sid:
                continue
            try:
                sd = request_json(api_key, f"/brewsessions/{sid}")
                enriched["brew_sessions_detail"].append(first_item(sd, "brewsessions") or sd)
            except Exception as exc:  # noqa: BLE001
                enriched["errors"].append(f"brew_session_detail:{sid}: {exc}")
            try:
                enriched["brew_sessions_logs"].append(request_json(api_key, f"/brewsessions/{sid}/logs"))
            except Exception as exc:  # noqa: BLE001
                enriched["errors"].append(f"brew_session_logs:{sid}: {exc}")

        full_export.append(enriched)
        safe_write_json(out_dir / "recipes" / recipe_id / "bundle.json", enriched)
        time.sleep(0.05)

    safe_write_json(out_dir / "all_recipes_full.json", full_export)
    manifest = {
        "recipe_count": len(recipes),
        "brew_session_count": len(sessions),
        "output_dir": str(out_dir.as_posix()),
    }
    safe_write_json(out_dir / "manifest.json", manifest)
    print("API export complete.", flush=True)
    print(json.dumps(manifest, indent=2), flush=True)
    return len(recipes)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export Brewers Friend recipes (web scrape or private API).",
    )
    parser.add_argument(
        "--source",
        choices=["web", "api", "sitemap"],
        default="web",
        help="web = scrape public pages (default), api = own recipes via API, "
             "sitemap = discover IDs then hit API per recipe.",
    )
    parser.add_argument("--output-dir", default="exports/brewersfriend")
    parser.add_argument("--max-recipes", type=int, default=0, help="0 = no limit")
    parser.add_argument("--workers", type=int, default=3, help="Concurrent request workers (web mode)")
    parser.add_argument("--delay", type=float, default=0.4, help="Delay between request submissions (s)")
    parser.add_argument("--sitemap-url", action="append", default=[])
    args = parser.parse_args()

    workspace = Path.cwd()
    load_env_files(workspace)
    out_dir = Path(args.output_dir)
    sitemap_urls = args.sitemap_url if args.sitemap_url else DEFAULT_RECIPE_SITEMAPS

    if args.source == "web":
        jsonl_path = out_dir / "recipes.jsonl"
        run_web_scrape(sitemap_urls, jsonl_path, args.max_recipes, args.workers, args.delay)
        return 0

    api_key = resolve_api_key(workspace)
    if not api_key:
        print("ERROR: Missing API key. Set bf_api_key in .env or environment.", file=sys.stderr, flush=True)
        return 1

    if args.source == "api":
        run_api_export(api_key, out_dir, args.max_recipes)
    elif args.source == "sitemap":
        print("Collecting sitemap URLs...", flush=True)
        recipe_urls = collect_sitemap_urls(sitemap_urls)
        if args.max_recipes > 0:
            recipe_urls = recipe_urls[:args.max_recipes]
        recipes: list[dict[str, Any]] = []
        for idx, url in enumerate(recipe_urls, 1):
            rid = extract_recipe_id(url)
            if not rid:
                continue
            try:
                p = request_json(api_key, f"/recipes/{rid}")
                recipes.append(first_item(p, "recipes") or p)
            except Exception as exc:  # noqa: BLE001
                recipes.append({"id": rid, "url": url, "error": str(exc)})
            if idx % 50 == 0:
                print(f"  [{idx}/{len(recipe_urls)}]", flush=True)
            time.sleep(0.05)
        safe_write_json(out_dir / "sitemap_recipes.json", recipes)
        print(f"Wrote {len(recipes)} recipes.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
