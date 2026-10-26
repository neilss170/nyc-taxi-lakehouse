from __future__ import annotations

from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


def trip(pickup: datetime, minutes: float = 12, **overrides) -> dict:
    row = {
        "VendorID": 2, "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=minutes),
        "passenger_count": 1, "trip_distance": 2.1, "RatecodeID": 1, "store_and_fwd_flag": "N",
        "PULocationID": 161, "DOLocationID": 237, "payment_type": 1, "fare_amount": 12.8,
        "extra": 1.0, "mta_tax": 0.5, "tip_amount": 3.0, "tolls_amount": 0.0,
        "improvement_surcharge": 1.0, "total_amount": 20.8, "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
    }
    row.update(overrides)
    return row


def write_month(lake_root, month: str, rows: list[dict], legacy_schema: bool = False) -> None:
    """Write a raw TLC-style file. legacy_schema mimics 2023-01 (int64/double types, lowercase airport_fee)."""
    int_type = pa.int64() if legacy_schema else pa.int32()
    count_type = pa.float64() if legacy_schema else pa.int64()
    fee_name = "airport_fee" if legacy_schema else "Airport_fee"
    fields = [
        ("VendorID", int_type), ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")), ("passenger_count", count_type),
        ("trip_distance", pa.float64()), ("RatecodeID", count_type), ("store_and_fwd_flag", pa.string()),
        ("PULocationID", int_type), ("DOLocationID", int_type), ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()), ("extra", pa.float64()), ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()), ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()), ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()), (fee_name, pa.float64()),
    ]
    columns = {
        name: pa.array([r.get("Airport_fee") if name == fee_name else r.get(name) for r in rows], type=t)
        for name, t in fields
    }
    pq.write_table(pa.table(columns), lake_root / "raw" / f"yellow_tripdata_{month}.parquet")
