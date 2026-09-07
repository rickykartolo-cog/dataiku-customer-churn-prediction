"""Join the source 80% and 20% datasets."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from churn_pipeline.io import INPUT_COLUMNS


def join_80_20(df80: DataFrame, df20: DataFrame) -> DataFrame:
    """Prefix both source datasets and inner join on State and Area code."""
    left = df80.select([F.col(column).alias(f"80_{column}") for column in INPUT_COLUMNS])
    right = df20.select([F.col(column).alias(f"20_{column}") for column in INPUT_COLUMNS])
    joined = left.join(
        right,
        (F.col("80_State") == F.col("20_State"))
        & (F.col("80_Area code") == F.col("20_Area code")),
        "inner",
    )
    return joined.select(
        [F.col(f"80_{column}") for column in INPUT_COLUMNS]
        + [F.col(f"20_{column}") for column in INPUT_COLUMNS]
    )
