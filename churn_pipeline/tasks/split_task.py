"""Databricks/local task for splitting prepared churn data into train and test."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from churn_pipeline.io import (
    DATASET_DIRS,
    PREPARED_SCHEMA,
    get_spark,
    read_dataset,
    write_dataset,
)
from churn_pipeline.recipes.split import split_train_test


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output_format", default="delta", choices=["delta", "parquet", "csv"])
    parser.add_argument("--base_output_path", default="/tmp/churn_pipeline")
    parser.add_argument("--prepared_path", default=None)
    parser.add_argument("--train_path", default=None)
    parser.add_argument("--test_path", default=None)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--train_share", type=float, default=0.8)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    prepared_path = args.prepared_path or str(
        Path(args.base_output_path) / DATASET_DIRS["prepared"]
    )
    train_path = args.train_path or str(
        Path(args.base_output_path) / DATASET_DIRS["train"]
    )
    test_path = args.test_path or str(
        Path(args.base_output_path) / DATASET_DIRS["test"]
    )
    spark = get_spark()
    prepared = read_dataset(
        spark,
        prepared_path,
        args.output_format,
        schema=PREPARED_SCHEMA if args.output_format == "csv" else None,
    )
    train, test = split_train_test(prepared, args.seed, args.train_share)
    write_dataset(train, train_path, args.output_format)
    write_dataset(test, test_path, args.output_format)
    print(f"Wrote {train.count()} rows x {len(train.columns)} columns to {train_path}")
    print(f"Wrote {test.count()} rows x {len(test.columns)} columns to {test_path}")


if __name__ == "__main__":
    main()
