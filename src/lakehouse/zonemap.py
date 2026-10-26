"""Taxi-zone boundaries -> simplified SVG paths for the dashboard map.

The TLC shapefile is in NY State Plane Long Island (EPSG:2263, US feet), a planar projection,
so it maps to SVG with a scale and a y-flip; no reprojection is needed. Rings are simplified
with Douglas-Peucker to keep the file small.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections import defaultdict

import shapefile
from pyspark.sql import SparkSession

from lakehouse import config, gold

VIEW_WIDTH = 1000
TOLERANCE_FT = 60  # simplification tolerance; ~0.3 px at the map's default size


def _perp_distance(p, a, b) -> float:
    (x, y), (x1, y1), (x2, y2) = p, a, b
    dx, dy = x2 - x1, y2 - y1
    if dx == dy == 0:
        return ((x - x1) ** 2 + (y - y1) ** 2) ** 0.5
    return abs(dy * x - dx * y + x2 * y1 - y2 * x1) / (dx * dx + dy * dy) ** 0.5


def simplify(points: list[tuple[float, float]], tolerance: float) -> list[tuple[float, float]]:
    """Douglas-Peucker, iterative so long coastlines cannot hit the recursion limit."""
    if len(points) < 3:
        return points
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        worst, index = 0.0, None
        for i in range(start + 1, end):
            d = _perp_distance(points[i], points[start], points[end])
            if d > worst:
                worst, index = d, i
        if index is not None and worst > tolerance:
            keep[index] = True
            stack += [(start, index), (index, end)]
    return [p for p, k in zip(points, keep) if k]


def _rings(shape) -> list[list[tuple[float, float]]]:
    bounds = list(shape.parts) + [len(shape.points)]
    return [shape.points[a:b] for a, b in zip(bounds, bounds[1:])]


def build(zip_bytes: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        stem = next(n[:-4] for n in z.namelist() if n.lower().endswith(".shp"))
        reader = shapefile.Reader(shp=io.BytesIO(z.read(stem + ".shp")), dbf=io.BytesIO(z.read(stem + ".dbf")),
                                  shx=io.BytesIO(z.read(stem + ".shx")))
        records = list(reader.iterShapeRecords())

    min_x, min_y, max_x, max_y = reader.bbox
    scale = VIEW_WIDTH / (max_x - min_x)
    height = round((max_y - min_y) * scale)

    # a few LocationIDs (e.g. 56, 103) are split across several records; merge them into one path
    paths: dict[int, list[str]] = defaultdict(list)
    names: dict[int, tuple[str, str]] = {}
    for rec in records:
        location_id = int(rec.record["LocationID"])
        names[location_id] = (rec.record["zone"], rec.record["borough"])
        for ring in _rings(rec.shape):
            pts = simplify(ring, TOLERANCE_FT)
            if len(pts) < 4:
                continue  # collapsed to a sliver
            xy = [(round((x - min_x) * scale, 1), round((max_y - y) * scale, 1)) for x, y in pts]
            paths[location_id].append("M" + "L".join(f"{x:g},{y:g}" for x, y in xy) + "Z")
    return {
        "width": VIEW_WIDTH,
        "height": height,
        "zones": [{"location_id": i, "zone": names[i][0], "borough": names[i][1], "d": "".join(paths[i])}
                  for i in sorted(paths)],
    }


def run(spark: SparkSession) -> dict:
    shapes = build((config.paths().raw / "taxi_zones.zip").read_bytes())
    gold.SITE_DATA.mkdir(parents=True, exist_ok=True)
    out = gold.SITE_DATA / "zone_shapes.json"
    out.write_text(json.dumps(shapes, separators=(",", ":")))
    return {"zones": len(shapes["zones"]), "bytes": out.stat().st_size, "tolerance_ft": TOLERANCE_FT}
