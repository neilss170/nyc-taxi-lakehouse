"""Paths and dataset scope, overridable through environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_LOOKUP_URL = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = REPO_ROOT / "reports"


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of YYYY-MM strings."""
    y, m = map(int, start.split("-"))
    end_y, end_m = map(int, end.split("-"))
    months = []
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> str:
        return str(self.root / "bronze" / "yellow_trips")

    @property
    def silver(self) -> str:
        return str(self.root / "silver" / "yellow_trips")

    @property
    def quarantine(self) -> str:
        return str(self.root / "silver" / "quarantine")

    @property
    def zones(self) -> str:
        return str(self.root / "silver" / "zones")

    def gold(self, table: str) -> str:
        return str(self.root / "gold" / table)


def paths() -> Paths:
    return Paths(Path(os.getenv("LAKEHOUSE_DATA", REPO_ROOT / "data")))


def months() -> list[str]:
    return month_range(os.getenv("LAKEHOUSE_START", "2023-01"), os.getenv("LAKEHOUSE_END", "2024-12"))


def raw_file_name(month: str) -> str:
    return f"yellow_tripdata_{month}.parquet"
