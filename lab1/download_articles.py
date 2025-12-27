from __future__ import annotations

import gzip
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

USER_AGENT = "Mozilla/5.0 (compatible; LabCollector/1.0)"
BASE = Path(__file__).resolve().parent
SAMPLES_DIR = BASE / "samples"


@dataclass
class Source:
    name: str
    sitemap: str
    pattern: re.Pattern[str]
    target: int = 5
    pool: int = 40
    prettify: bool = False  # if True, insert line breaks between tags for readability


SOURCES: list[Source] = [
    Source(
        name="interfax",
        sitemap="https://www.interfax.ru/SEO_SiteMapIndex.xml",
        pattern=re.compile(r"https://www\.interfax\.ru/[^/]+/\d+$"),
        target=5,
        pool=20,
        prettify=False,
    ),
    Source(
        name="tass",
        sitemap="https://tass.ru/sitemap.xml",
        pattern=re.compile(r"https://tass\.ru/[^/]+/\d+$"),
        target=5,
        pool=40,
        prettify=True,
    ),
]

HEADERS = {"User-Agent": USER_AGENT}


class SimpleHTMLTextExtractor:
    """Very small HTML text extractor that skips script/style/noscript."""

    def __init__(self) -> None:
        from html.parser import HTMLParser

        class _Parser(HTMLParser):
            def __init__(self) -> None:
                super().__init__()
                self.chunks: List[str] = []
                self.skip = False

            def handle_starttag(self, tag, attrs):
                if tag in {"script", "style", "noscript"}:
                    self.skip = True

            def handle_endtag(self, tag):
                if tag in {"script", "style", "noscript"}:
                    self.skip = False

            def handle_data(self, data):
                if not self.skip:
                    t = data.strip()
                    if t:
                        self.chunks.append(t)

        self._parser = _Parser()

    def feed(self, html: str) -> None:
        self._parser.feed(html)

    def get_text(self) -> str:
        return " ".join(self._parser.chunks)


def maybe_decompress(data: bytes, headers) -> bytes:
    enc = (headers.get("Content-Encoding") or "").lower()
    if enc == "gzip" or data[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(data)
        except Exception:
            return data
    return data


def fetch_xml(url: str) -> ET.Element:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = maybe_decompress(resp.read(), resp.headers)
    return ET.fromstring(data)


def collect_article_links(sitemap_url: str, pattern: re.Pattern[str], limit: int, visited: set[str]) -> List[str]:
    if limit <= 0 or sitemap_url in visited:
        return []
    visited.add(sitemap_url)
    root = fetch_xml(sitemap_url)
    links: List[str] = []
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for loc_el in root.findall(".//{*}loc"):
            loc_text = (loc_el.text or "").strip()
            if not loc_text:
                continue
            links.extend(collect_article_links(loc_text, pattern, limit - len(links), visited))
            if len(links) >= limit:
                break
    else:
        for loc_el in root.findall(".//{*}loc"):
            href = (loc_el.text or "").strip()
            if not href:
                continue
            if pattern.match(href):
                links.append(href)
                if len(links) >= limit:
                    break
    return links


def prettify_html(text: str) -> str:
    """Insert newlines between tags to avoid one-line HTML (good enough for readability)."""
    return re.sub(r">\s*<", ">\n<", text)


def download_html(url: str) -> bytes:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = maybe_decompress(resp.read(), resp.headers)
    return data


def save_article(source: Source, idx: int, url: str) -> tuple[int, int]:
    dest = SAMPLES_DIR / source.name / f"{source.name}_{idx}.html"
    dest.parent.mkdir(parents=True, exist_ok=True)
    html_bytes = download_html(url)
    if source.prettify:
        text = html_bytes.decode("utf-8", errors="ignore")
        text = prettify_html(text)
        html_bytes = text.encode("utf-8")
    dest.write_bytes(html_bytes)
    extractor = SimpleHTMLTextExtractor()
    extractor.feed(html_bytes.decode("utf-8", errors="ignore"))
    text_len = len(extractor.get_text())
    return len(html_bytes), text_len


def main(sources: Iterable[Source] = SOURCES) -> None:
    BASE.mkdir(parents=True, exist_ok=True)
    summary = []
    for source in sources:
        candidates = collect_article_links(source.sitemap, source.pattern, source.pool, set())
        downloaded: list[str] = []
        raw_sizes: list[int] = []
        text_sizes: list[int] = []
        for url in candidates:
            if len(downloaded) >= source.target:
                break
            try:
                raw_size, text_size = save_article(source, len(downloaded) + 1, url)
            except Exception as exc:  # noqa: BLE001
                print(f"skip {url} because {exc}")
                continue
            downloaded.append(url)
            raw_sizes.append(raw_size)
            text_sizes.append(text_size)
        (BASE / f"{source.name}_urls.txt").write_text("\n".join(downloaded), encoding="utf-8")
        summary.append(
            {
                "name": source.name,
                "count": len(downloaded),
                "raw_total_bytes": sum(raw_sizes),
                "raw_avg_bytes": sum(raw_sizes) / len(raw_sizes) if raw_sizes else 0,
                "text_total_chars": sum(text_sizes),
                "text_avg_chars": sum(text_sizes) / len(text_sizes) if text_sizes else 0,
            }
        )
    print("Collected samples:")
    for item in summary:
        print(item)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
