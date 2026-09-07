"""Prepare the joined churn dataset."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from churn_pipeline.io import PREPARED_COLUMNS


def prepare_joined(df: DataFrame) -> DataFrame:
    """Add aggregate columns, apply the DSS charge range filter, and order columns."""
    prepared = (
        df.withColumn(
            "80_total minutes",
            F.col("80_Total day minutes").cast("double")
            + F.col("80_Total eve minutes").cast("double")
            + F.col("80_Total night minutes").cast("double"),
        )
        .withColumn(
            "80_Total calls",
            F.col("80_Total day calls").cast("long")
            + F.col("80_Total eve calls").cast("long")
            + F.col("80_Total night calls").cast("long"),
        )
    )
    day_charge = F.col("80_Total day charge").cast("double")
    return prepared.where(
        day_charge.isNotNull() & (day_charge >= F.lit(3.0)) & (day_charge <= F.lit(60.0))
    ).select(PREPARED_COLUMNS)
