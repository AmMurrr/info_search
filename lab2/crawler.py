from __future__ import annotations

import argparse
import gzip
import hashlib
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional
from urllib.parse import urlsplit, urlunsplit

import pymongo
import yaml

# Minimal comments: key points only.


@dataclass
class Source:
    name: str
    sitemap: str
    pattern: re.Pattern[str]
    target: int
    pool: int


@dataclass
class Config:
    uri: str
    database: str
    documents_collection: str
    queue_collection: str
    checkpoints_collection: str
    sources: list[Source]
    delay_seconds: float
    user_agent: str
    recrawl_hours: int
    batch_size: int


def load_config(path: Path) -> Config:
    raw = yaml.safe_load(path.read_text())
    logic = raw["logic"]
    sources = [
        Source(
            name=s["name"],
            sitemap=s["sitemap"],
            pattern=re.compile(s["pattern"]),
            target=int(s.get("target", 0)),
            pool=int(s.get("pool", s.get("target", 0))),
        )
        for s in logic["sources"]
    ]
    db = raw["db"]
    return Config(
        uri=db["uri"],
        database=db["database"],
        documents_collection=db["documents_collection"],
        queue_collection=db["queue_collection"],
        checkpoints_collection=db.get("checkpoints_collection", "checkpoints"),
        sources=sources,
        delay_seconds=float(logic.get("delay_seconds", 1.0)),
        user_agent=str(logic.get("user_agent", "Mozilla/5.0")),
        recrawl_hours=int(logic.get("recrawl_hours", 72)),
        batch_size=int(logic.get("batch_size", 50)),
    )


def normalize_url(url: str) -> str:
    parts = urlsplit(url)
    netloc = parts.netloc.lower()
    path = parts.path
    return urlunsplit((parts.scheme, netloc, path, "", ""))


def maybe_decompress(data: bytes, headers) -> bytes:
    enc = (headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or data[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(data)
        except Exception:
            return data
    return data


def fetch_xml(url: str, user_agent: str) -> ET.Element:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = maybe_decompress(resp.read(), resp.headers)
    return ET.fromstring(data)


def iter_sitemap_urls(root_url: str, pattern: re.Pattern[str], limit: int, user_agent: str, visited: Optional[set[str]] = None, remaining: Optional[list[int]] = None) -> Iterator[str]:
    # remaining is a single-item list to share mutable counter across recursion
    if visited is None:
        visited = set()
    if remaining is None:
        remaining = [limit]
    if remaining[0] <= 0 or root_url in visited:
        return
    visited.add(root_url)
    try:
        root = fetch_xml(root_url, user_agent)
    except Exception as exc:  # noqa: BLE001
        print(f"skip sitemap {root_url}: {exc}")
        return
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for loc_el in root.findall(".//{*}loc"):
            if remaining[0] <= 0:
                break
            loc_text = (loc_el.text or "").strip()
            if not loc_text:
                continue
            yield from iter_sitemap_urls(loc_text, pattern, limit, user_agent, visited, remaining)
    else:
        for loc_el in root.findall(".//{*}loc"):
            if remaining[0] <= 0:
                break
            href = (loc_el.text or "").strip()
            if not href:
                continue
            if pattern.match(href):
                yield href
                remaining[0] -= 1


def enqueue_urls(cfg: Config, client: pymongo.MongoClient, urls: Iterable[str], source: str, progress_step: int = 1000, max_insert: Optional[int] = None) -> int:
    db = client[cfg.database]
    queue = db[cfg.queue_collection]
    docs = db[cfg.documents_collection]
    inserted = 0
    seen = 0
    for url in urls:
        if max_insert is not None and inserted >= max_insert:
            break
        seen += 1
        norm = normalize_url(url)
        if docs.count_documents({"url": norm}, limit=1):
            continue
        queue.update_one(
            {"url": norm},
            {
                "$setOnInsert": {
                    "url": norm,
                    "source": source,
                    "status": "pending",
                    "error": None,
                    "created_at": int(time.time()),
                }
            },
            upsert=True,
        )
        inserted += 1
        if progress_step and inserted % progress_step == 0:
            print(f"enqueue progress [{source}]: {inserted} inserted (seen {seen})")
    return inserted


def collect_and_enqueue(cfg: Config) -> None:
    client = pymongo.MongoClient(cfg.uri)
    total_inserted = 0
    for src in cfg.sources:
        urls = iter_sitemap_urls(src.sitemap, src.pattern, src.pool, cfg.user_agent)
        inserted = enqueue_urls(cfg, client, urls, src.name, max_insert=src.target)
        total_inserted += inserted
        print(f"enqueued from {src.name}: {inserted}")
    print(f"total enqueued: {total_inserted}")


def fetch_once(cfg: Config, queue_doc: dict, client: pymongo.MongoClient) -> None:
    db = client[cfg.database]
    queue = db[cfg.queue_collection]
    docs = db[cfg.documents_collection]
    url = queue_doc["url"]
    req = urllib.request.Request(url, headers={"User-Agent": cfg.user_agent})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            html_bytes = maybe_decompress(resp.read(), resp.headers)
    except urllib.error.HTTPError as exc:  # noqa: PERF203
        now = int(time.time())
        if exc.code == 404:
            queue.update_one(
                {"_id": queue_doc["_id"]},
                {"$set": {"status": "done", "error": str(exc), "updated_at": now}},
            )
            return
        raise
    content_hash = hashlib.sha256(html_bytes).hexdigest()
    now = int(time.time())
    existing = docs.find_one({"url": url}, projection={"content_hash": 1})
    if existing and existing.get("content_hash") == content_hash:
        queue.update_one({"_id": queue_doc["_id"]}, {"$set": {"status": "done", "error": None, "updated_at": now}})
        return
    docs.update_one(
        {"url": url},
        {
            "$set": {
                "url": url,
                "source": queue_doc.get("source"),
                "raw_html": html_bytes.decode("utf-8", errors="ignore"),
                "fetched_at": now,
                "content_hash": content_hash,
            }
        },
        upsert=True,
    )
    queue.update_one({"_id": queue_doc["_id"]}, {"$set": {"status": "done", "error": None, "updated_at": now}})


def fetch_loop(cfg: Config) -> None:
    client = pymongo.MongoClient(cfg.uri)
    db = client[cfg.database]
    queue = db[cfg.queue_collection]
    # unlock any stuck "running" tasks from previous aborted runs
    queue.update_many({"status": "running"}, {"$set": {"status": "pending"}})
    total = queue.count_documents({"status": "pending"})
    processed = 0
    while True:
        doc = queue.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "running", "started_at": int(time.time())}},
        )
        if not doc:
            print("queue empty")
            break
        try:
            fetch_once(cfg, doc, client)
        except Exception as exc:  # noqa: BLE001
            queue.update_one({"_id": doc["_id"]}, {"$set": {"status": "pending", "error": str(exc)}})
            print(f"fetch error, returned to pending: {doc['url']} -> {exc}")
        processed += 1
        if total:
            print(f"progress: {processed}/{total} ({doc['url']})")
        time.sleep(cfg.delay_seconds)


def schedule_recrawl(cfg: Config) -> None:
    client = pymongo.MongoClient(cfg.uri)
    db = client[cfg.database]
    queue = db[cfg.queue_collection]
    docs = db[cfg.documents_collection]
    threshold = int(time.time()) - cfg.recrawl_hours * 3600
    cursor = docs.find({"fetched_at": {"$lt": threshold}}, projection={"url": 1, "source": 1})
    added = 0
    for doc in cursor:
        queue.update_one(
            {"url": doc["url"]},
            {"$set": {"status": "pending", "source": doc.get("source")}, "$setOnInsert": {"created_at": int(time.time())}},
            upsert=True,
        )
        added += 1
    print(f"scheduled for recrawl: {added}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Lab2 crawler")
    p.add_argument("--config", required=True, type=Path, help="YAML config path")
    p.add_argument("--mode", required=True, choices=["enqueue", "fetch", "recrawl"], help="what to run")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    if args.mode == "enqueue":
        collect_and_enqueue(cfg)
    elif args.mode == "fetch":
        fetch_loop(cfg)
    elif args.mode == "recrawl":
        schedule_recrawl(cfg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
