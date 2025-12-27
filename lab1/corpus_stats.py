from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
from typing import List

BASE = Path(__file__).resolve().parent
SAMPLES_DIR = BASE / "samples"


class TextExtractor(HTMLParser):
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

    def get_text(self) -> str:
        return " ".join(self.chunks)


def text_len(data: bytes) -> int:
    parser = TextExtractor()
    parser.feed(data.decode("utf-8", errors="ignore"))
    return len(parser.get_text())


def collect_stats(source_dir: Path) -> dict:
    raw_total = 0
    text_total = 0
    files = []
    for path in sorted(source_dir.glob("*.html")):
        raw = path.read_bytes()
        raw_len = len(raw)
        text_len_val = text_len(raw)
        raw_total += raw_len
        text_total += text_len_val
        files.append((path.name, raw_len, text_len_val))
    count = len(files)
    return {
        "count": count,
        "raw_total": raw_total,
        "raw_avg": raw_total / count if count else 0,
        "text_total": text_total,
        "text_avg": text_total / count if count else 0,
        "files": files,
    }


def main() -> None:
    for name in ["interfax", "tass"]:
        stats = collect_stats(SAMPLES_DIR / name)
        print(name)
        print(stats)


if __name__ == "__main__":
    main()
