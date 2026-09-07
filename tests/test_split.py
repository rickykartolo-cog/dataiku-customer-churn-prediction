from pathlib import Path

import pytest
from pyspark.sql import functions as F

from churn_pipeline.io import read_source_csv
from churn_pipeline.recipes.join import join_80_20
from churn_pipeline.recipes.prepare import prepare_joined
from churn_pipeline.recipes.split import split_train_test


REPO_ROOT = Path(__file__).resolve().parents[1]


def prepared_fixture(spark):
    df80 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_80/churn-bigml-80.csv")
    df20 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_20/churn-bigml-20.csv")
    return prepare_joined(join_80_20(df80, df20)).withColumn(
        "_row_id", F.monotonically_increasing_id()
    )


def test_split_counts_share_disjointness_and_determinism(spark):
    prepared = prepared_fixture(spark).cache()
    train1, test1 = split_train_test(prepared)
    train2, test2 = split_train_test(prepared)

    assert train1.count() + test1.count() == prepared.count()
    assert train1.count() / prepared.count() == pytest.approx(0.8, abs=0.02)
    train_ids = {row["_row_id"] for row in train1.select("_row_id").collect()}
    test_ids = {row["_row_id"] for row in test1.select("_row_id").collect()}
    assert train_ids.isdisjoint(test_ids)
    assert train_ids == {row["_row_id"] for row in train2.select("_row_id").collect()}
    assert test_ids == {row["_row_id"] for row in test2.select("_row_id").collect()}
