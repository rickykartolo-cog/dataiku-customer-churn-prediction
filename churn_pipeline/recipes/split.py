"""Deterministically split prepared data into train and test datasets."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def split_train_test(
    df: DataFrame, seed: int = 1337, train_share: float = 0.8
) -> tuple[DataFrame, DataFrame]:
    """Split rows by a seeded random value after coalescing to one partition."""
    split_df = df.coalesce(1).withColumn("_split_rnd", F.rand(seed))
    train = split_df.where(F.col("_split_rnd") < train_share).drop("_split_rnd")
    test = split_df.where(F.col("_split_rnd") >= train_share).drop("_split_rnd")
    return train, test
