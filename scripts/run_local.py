"""Run the whole pipeline on your machine: master -> landing -> bronze -> silver -> gold.

    python scripts/run_local.py               # fresh run in ./local_lake
    python scripts/run_local.py --replay      # drop the same files again to test dedup
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mfg_etl.bronze import run_bronze  # noqa: E402
from mfg_etl.config import Paths  # noqa: E402
from mfg_etl.gold import run_gold  # noqa: E402
from mfg_etl.local_spark import get_local_spark  # noqa: E402
from mfg_etl.master_data import write_master_data  # noqa: E402
from mfg_etl.silver import run_silver_production, run_silver_telemetry  # noqa: E402
from mfg_etl.simulator import write_sample_files  # noqa: E402
from mfg_etl.validation import layer_counts, reconcile  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", default=str(ROOT / "local_lake"), help="local folder for the data lake")
    parser.add_argument("--replay", action="store_true", help="keep existing data and send the files again")
    args = parser.parse_args()

    if not args.replay and Path(args.lake).exists():
        shutil.rmtree(args.lake)  # fresh lab run: reset data AND checkpoints together

    spark = get_local_spark()
    paths = Paths.for_local(args.lake)

    print("master data:", write_master_data(spark, paths))
    write_sample_files(spark, paths)
    run_bronze(spark, paths)
    run_silver_telemetry(spark, paths)
    run_silver_production(spark, paths)
    print("gold tables:", run_gold(spark, paths))
    print("layer counts:", layer_counts(spark, paths))
    print("reconciliation:", reconcile(spark, paths))

    for name in ["hourly_production", "hourly_anomalies", "batch_quality", "data_quality_summary"]:
        print(f"\n== gold/{name}")
        df = spark.read.format("delta").load(paths.gold(name))
        df.orderBy(df.columns[0]).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
