"""Download TLC yellow-taxi monthly Parquet files and the zone lookup, with a checksum manifest."""

from __future__ import annotations

import hashlib
import json
import time
import urllib.request
from pathlib import Path

from lakehouse import config


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch(url: str, dest: Path, attempts: int = 4) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp, tmp.open("wb") as out:
                expected = int(resp.headers.get("Content-Length", 0))
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
            if expected and tmp.stat().st_size != expected:
                raise OSError(f"short read: {tmp.stat().st_size} of {expected} bytes")
            tmp.replace(dest)
            return
        except OSError as exc:
            if attempt == attempts:
                raise
            print(f"  retry {attempt} for {url}: {exc}")
            time.sleep(2 * attempt)


def download(months: list[str] | None = None) -> dict:
    raw = config.paths().raw
    raw.mkdir(parents=True, exist_ok=True)
    manifest_path = raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}

    targets = {config.raw_file_name(m): f"{config.TLC_BASE_URL}/trip-data/{config.raw_file_name(m)}"
               for m in months or config.months()}
    targets["taxi_zone_lookup.csv"] = config.ZONE_LOOKUP_URL

    for name, url in targets.items():
        dest = raw / name
        if dest.exists() and manifest.get(name, {}).get("sha256") == _sha256(dest):
            continue
        print(f"downloading {name}")
        _fetch(url, dest)
        manifest[name] = {"url": url, "bytes": dest.stat().st_size, "sha256": _sha256(dest)}
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest
