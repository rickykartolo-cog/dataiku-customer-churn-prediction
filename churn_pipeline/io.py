"""Shared input, output, and schema definitions for the churn pipeline."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

INPUT_COLUMNS = [
    "State",
    "Account length",
    "Area code",
    "International plan",
    "Voice mail plan",
    "Number vmail messages",
    "Total day minutes",
    "Total day calls",
    "Total day charge",
    "Total eve minutes",
    "Total eve calls",
    "Total eve charge",
    "Total night minutes",
    "Total night calls",
    "Total night charge",
    "Total intl minutes",
    "Total intl calls",
    "Total intl charge",
    "Customer service calls",
    "Churn",
]

DATASET_DIRS = {
    "joined": "churn_bigml_80_joined",
    "prepared": "churn_bigml_80_joined_prepared",
    "train": "churn_data_train",
    "test": "churn_data_test",
}

PREPARED_COLUMNS = [
    "80_State",
    "80_Account length",
    "80_Area code",
    "80_International plan",
    "80_Voice mail plan",
    "80_Number vmail messages",
    "80_Total day minutes",
    "80_Total day calls",
    "80_Total day charge",
    "80_Total eve minutes",
    "80_Total eve calls",
    "80_Total eve charge",
    "80_Total night minutes",
    "80_total minutes",
    "80_Total night calls",
    "80_Total calls",
    "80_Total night charge",
    "80_Total intl minutes",
    "80_Total intl calls",
    "80_Total intl charge",
    "80_Customer service calls",
    "80_Churn",
    "20_State",
    "20_Account length",
    "20_Area code",
    "20_International plan",
    "20_Voice mail plan",
    "20_Number vmail messages",
    "20_Total day minutes",
    "20_Total day calls",
    "20_Total day charge",
    "20_Total eve minutes",
    "20_Total eve calls",
    "20_Total eve charge",
    "20_Total night minutes",
    "20_Total night calls",
    "20_Total night charge",
    "20_Total intl minutes",
    "20_Total intl calls",
    "20_Total intl charge",
    "20_Customer service calls",
    "20_Churn",
]

JOINED_COLUMNS = [f"80_{column}" for column in INPUT_COLUMNS] + [
    f"20_{column}" for column in INPUT_COLUMNS
]

JOINED_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in JOINED_COLUMNS]
)
PREPARED_SCHEMA = StructType(
    [
        StructField(
            column,
            DoubleType() if column == "80_total minutes" else LongType()
            if column == "80_Total calls"
            else StringType(),
            True,
        )
        for column in PREPARED_COLUMNS
    ]
)


def read_source_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a DSS-style source CSV, retaining every field as a string."""
    df = (
        spark.read.option("header", True)
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", False)
        .csv(str(path))
    )
    if df.columns != INPUT_COLUMNS:
        raise ValueError(f"Unexpected source columns: {df.columns!r}")
    return df


def write_dataset(df: DataFrame, path: str, fmt: str = "delta") -> None:
    """Overwrite a dataset in Delta, Parquet, or CSV format."""
    if fmt not in {"delta", "parquet", "csv"}:
        raise ValueError(f"Unsupported dataset format: {fmt}")
    writer = df.write.mode("overwrite")
    if fmt == "delta":
        writer.option("overwriteSchema", "true").format(fmt).save(path)
    elif fmt == "csv":
        writer.option("header", True).format(fmt).save(path)
    else:
        writer.format(fmt).save(path)


def read_dataset(
    spark: SparkSession, path: str, fmt: str = "delta", schema: StructType | None = None
) -> DataFrame:
    """Read a dataset, using an explicit schema for CSV when supplied."""
    if fmt not in {"delta", "parquet", "csv"}:
        raise ValueError(f"Unsupported dataset format: {fmt}")
    reader = spark.read
    if fmt == "csv":
        reader = (
            reader.option("header", True)
            .option("sep", ",")
            .option("quote", '"')
            .option("escape", '"')
        )
        if schema is not None:
            reader = reader.schema(schema)
    return reader.format(fmt).load(path)


def get_spark() -> SparkSession:
    """Return the active Spark session or create one locally."""
    return SparkSession.builder.getOrCreate()
