#!/usr/bin/env python3
"""Splice target/format-explorer/fixtures.json into docs/format-explorer/index.html.

The embedded <script id="fixtures-data"> block in index.html is replaced
in-place. The fixture JSON is the output of FormatExplorerFixtures.

Usage (from the fileio-catalog/ project root):
    mvn test -Dtest=FormatExplorerFixtures -Dexplorer.regenerate=true
    python3 docs/format-explorer/splice-fixtures.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HTML = ROOT / "docs" / "format-explorer" / "index.html"
JSON = ROOT / "target" / "format-explorer" / "fixtures.json"

if not HTML.exists():
    sys.exit(f"missing {HTML}")
if not JSON.exists():
    sys.exit(f"missing {JSON} -- run the FormatExplorerFixtures test first")

fixtures = JSON.read_text(encoding="utf-8").strip()
html = HTML.read_text(encoding="utf-8")

pattern = re.compile(
    r'(<script id="fixtures-data"[^>]*>)(.*?)(</script>)',
    re.DOTALL,
)
match = pattern.search(html)
if not match:
    sys.exit("could not find <script id=\"fixtures-data\"> block in index.html")

new_html = html[:match.start(2)] + fixtures + html[match.end(2):]
if new_html == html:
    print("no changes (fixtures already current)")
    sys.exit(0)
HTML.write_text(new_html, encoding="utf-8")
print(f"updated {HTML.relative_to(ROOT)} ({len(fixtures)} bytes of fixtures embedded)")
