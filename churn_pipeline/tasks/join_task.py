"""Databricks/local task for joining the two source datasets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from churn_pipeline.io import get_spark, read_source_csv, write_dataset
from churn_pipeline.recipes.join import join_80_20


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output_format", default="delta", choices=["delta", "parquet", "csv"])
    parser.add_argument("--base_output_path", default="/tmp/churn_pipeline")
    parser.add_argument(
        "--input_80_path",
        default=str(REPO_ROOT / "uploads/churn_bigml_80/churn-bigml-80.csv"),
    )
    parser.add_argument(
        "--input_20_path",
        default=str(REPO_ROOT / "uploads/churn_bigml_20/churn-bigml-20.csv"),
    )
    parser.add_argument("--joined_path", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    joined_path = args.joined_path or str(Path(args.base_output_path) / "joined")
    spark = get_spark()
    result = join_80_20(
        read_source_csv(spark, args.input_80_path),
        read_source_csv(spark, args.input_20_path),
    )
    write_dataset(result, joined_path, args.output_format)
    print(f"Wrote {result.count()} rows x {len(result.columns)} columns to {joined_path}")


if __name__ == "__main__":
    main()
