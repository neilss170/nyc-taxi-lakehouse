"""CLI: python -m lakehouse <stage> [<stage> ...]   (stages run in the order given)."""

from __future__ import annotations

import argparse
import json
import time

from lakehouse import config

STAGES = ["download", "bronze", "silver", "gold", "streaming", "forecast", "benchmark", "vacuum"]


def _write_report(name: str, payload: dict) -> None:
    config.REPORTS_DIR.mkdir(exist_ok=True)
    (config.REPORTS_DIR / f"{name}.json").write_text(json.dumps(payload, indent=2, default=str) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument("stages", nargs="+", choices=STAGES + ["all"])
    args = parser.parse_args()
    stages = STAGES if "all" in args.stages else args.stages

    spark = None
    for stage in stages:
        started = time.perf_counter()
        if stage == "download":
            from lakehouse.download import download
            result = {"files": download()}
        else:
            if spark is None:
                from lakehouse.spark import get_spark
                spark = get_spark()
            module = __import__(f"lakehouse.{stage}", fromlist=["run"])
            result = module.run(spark)
        result["seconds"] = round(time.perf_counter() - started, 1)
        _write_report(stage, result)
        print(f"[{stage}] done in {result['seconds']}s")
    if spark is not None:
        spark.stop()


if __name__ == "__main__":
    main()
