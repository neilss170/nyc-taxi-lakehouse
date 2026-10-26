from __future__ import annotations

import pytest

from lakehouse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests", conf={"spark.sql.shuffle.partitions": "4", "spark.ui.enabled": "false"})
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """An empty lakehouse root with a zone lookup, wired in through LAKEHOUSE_DATA."""
    monkeypatch.setenv("LAKEHOUSE_DATA", str(tmp_path))
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "taxi_zone_lookup.csv").write_text(
        '"LocationID","Borough","Zone","service_zone"\n'
        '1,"EWR","Newark Airport","EWR"\n'
        '132,"Queens","JFK Airport","Airports"\n'
        '161,"Manhattan","Midtown Center","Yellow Zone"\n'
        '237,"Manhattan","Upper East Side South","Yellow Zone"\n'
        '264,"Unknown","N/A","N/A"\n'
    )
    return tmp_path
