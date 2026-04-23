"""Download missing librdkafka CONFIGURATION.md snapshots, then run generate.py.

Convention: `versions/<X.Y.0>/CONFIGURATION.md`. Only minor tags (vX.Y.0) are tracked.
"""

from __future__ import annotations

import re
import sys
import json
import logging
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

REPO = "confluentinc/librdkafka"
TAGS_URL = f"https://api.github.com/repos/{REPO}/tags"
RAW_URL_TMPL = f"https://raw.githubusercontent.com/{REPO}/{{tag}}/CONFIGURATION.md"
SEMVER_MINOR_RE = re.compile(r"^v(\d+)\.(\d+)\.0$")

HERE = Path(__file__).parent
VERSIONS_DIR = HERE / "versions"
GENERATE_PY = HERE / "generate.py"


def fetch_minor_tags() -> list[tuple[tuple[int, int], str]]:
    tags: list[tuple[tuple[int, int], str]] = []
    for page in range(1, 11):
        url = f"{TAGS_URL}?per_page=100&page={page}"
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
        with urllib.request.urlopen(req) as resp:
            batch = json.load(resp)
        if not batch:
            break
        for tag in batch:
            match = SEMVER_MINOR_RE.match(tag["name"])
            if match:
                tags.append(((int(match.group(1)), int(match.group(2))), tag["name"]))
        if len(batch) < 100:
            break
    return tags


def download_configuration(tag: str) -> str:
    url = RAW_URL_TMPL.format(tag=tag)
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode("utf-8")


def main() -> int:
    existing = {p.name for p in VERSIONS_DIR.iterdir() if p.is_dir()}
    logger.info(f"Existing versions: {sorted(existing)}")

    existing_keys = {tuple(int(part) for part in v.split(".")[:2]) for v in existing}
    if not existing_keys:
        raise RuntimeError(f"{VERSIONS_DIR} is empty — nothing to compare against")
    floor = min(existing_keys)

    remote = [entry for entry in fetch_minor_tags() if entry[0] >= floor]
    logger.info(f"Remote minor tags (>= v{floor[0]}.{floor[1]}.0): {[name for _, name in remote]}")

    missing = [(key, name) for key, name in remote if f"{key[0]}.{key[1]}.0" not in existing]
    if not missing:
        logger.info("Nothing to download.")
    else:
        logger.info(f"Missing versions: {[name for _, name in missing]}")

    for (major, minor), tag in missing:
        version = f"{major}.{minor}.0"
        try:
            content = download_configuration(tag)
        except urllib.error.HTTPError as exc:
            logger.error(f"Skipping {tag}: HTTP {exc.code}")
            continue
        dst = VERSIONS_DIR / version
        dst.mkdir(parents=True, exist_ok=True)
        (dst / "CONFIGURATION.md").write_text(content)
        logger.info(f"Wrote {dst / 'CONFIGURATION.md'}")

    logger.info(f"Running {GENERATE_PY}...")
    return subprocess.run([sys.executable, str(GENERATE_PY)], cwd=HERE).returncode


if __name__ == "__main__":
    sys.exit(main())
