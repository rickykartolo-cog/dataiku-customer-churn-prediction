"""Databricks/local task for preparing the joined churn dataset."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from churn_pipeline.io import JOINED_SCHEMA, get_spark, read_dataset, write_dataset
from churn_pipeline.recipes.prepare import prepare_joined


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output_format", default="delta", choices=["delta", "parquet", "csv"])
    parser.add_argument("--base_output_path", default="/tmp/churn_pipeline")
    parser.add_argument("--joined_path", default=None)
    parser.add_argument("--prepared_path", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    joined_path = args.joined_path or str(Path(args.base_output_path) / "joined")
    prepared_path = args.prepared_path or str(Path(args.base_output_path) / "prepared")
    spark = get_spark()
    joined = read_dataset(
        spark,
        joined_path,
        args.output_format,
        schema=JOINED_SCHEMA if args.output_format == "csv" else None,
    )
    result = prepare_joined(joined)
    write_dataset(result, prepared_path, args.output_format)
    print(f"Wrote {result.count()} rows x {len(result.columns)} columns to {prepared_path}")


if __name__ == "__main__":
    main()
