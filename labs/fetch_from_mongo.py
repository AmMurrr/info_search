#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, List

from pymongo import MongoClient

# Order of fields to try for text. Override with --text-fields.
DEFAULT_TEXT_FIELDS: List[str] = ["raw_html", "html", "content", "text", "body"]


def pick_text_field(doc: dict, fields: List[str]) -> str:
    for key in fields:
        if key in doc and doc[key]:
            return doc[key]
    return ""


def export_documents(docs: Iterable[dict], out_path: Path, text_fields: List[str], progress_every: int) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            payload = {
                "source": doc.get("source") or doc.get("collection") or "mongo",
                "url": doc.get("url", ""),
                "title": doc.get("title", ""),
            }
            text = pick_text_field(doc, text_fields)
            if not text:
                continue
            payload["raw_html"] = text
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            count += 1
            if progress_every and count % progress_every == 0:
                print(f"  [PROGRESS] exported {count} docs", flush=True)
    return count


def main():
    parser = argparse.ArgumentParser(description="Export documents from MongoDB to NDJSON")
    parser.add_argument("--uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"), help="MongoDB connection URI")
    parser.add_argument("--database", default="searchdoc", help="Database name")
    parser.add_argument("--collection", default="documents", help="Collection name")
    parser.add_argument("--limit", type=int, default=0, help="Max documents to export (0 = all)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Cursor batch size")
    parser.add_argument("--text-fields", type=str, default=",".join(DEFAULT_TEXT_FIELDS), help="Comma-separated list of fields to use as text")
    parser.add_argument("--out", default="data/all_docs.ndjson", help="Output NDJSON path")
    parser.add_argument("--progress-every", type=int, default=5000, help="Print progress every N exported docs (0 = silent)")
    args = parser.parse_args()

    text_fields = [f.strip() for f in args.text_fields.split(",") if f.strip()]
    if not text_fields:
        text_fields = DEFAULT_TEXT_FIELDS

    client = MongoClient(args.uri)
    db = client[args.database]
    coll = db[args.collection]

    print(f"[INFO] Connecting to {args.uri} db={args.database} collection={args.collection}")
    print(f"[INFO] Text fields priority: {text_fields}")

    # limit=0 means no limit in PyMongo
    cursor = coll.find({}, projection=text_fields + ["url", "title", "source", "collection"],
                       batch_size=args.batch_size, no_cursor_timeout=True, limit=args.limit)

    out_path = Path(args.out)
    exported = export_documents(cursor, out_path, text_fields, args.progress_every)

    print(f"[DONE] Exported {exported} documents to {out_path}")
    if exported == 0:
        print("[WARN] No documents contained usable text fields. Check field names or increase limit.")


if __name__ == "__main__":
    main()
