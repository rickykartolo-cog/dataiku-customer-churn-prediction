from pathlib import Path

from churn_pipeline.io import JOINED_SCHEMA, PREPARED_COLUMNS, read_source_csv
from churn_pipeline.recipes.join import join_80_20
from churn_pipeline.recipes.prepare import prepare_joined


REPO_ROOT = Path(__file__).resolve().parents[1]


def joined_fixture(spark):
    df80 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_80/churn-bigml-80.csv")
    df20 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_20/churn-bigml-20.csv")
    return join_80_20(df80, df20)


def test_prepare_schema_values_and_filter(spark):
    prepared = prepare_joined(joined_fixture(spark))
    assert prepared.columns == PREPARED_COLUMNS
    assert prepared.count() == 13_184
    assert prepared.schema["80_total minutes"].dataType.typeName() == "double"
    assert prepared.schema["80_Total calls"].dataType.typeName() == "long"

    first = prepared.where("80_State = 'KS'").first()
    assert abs(first["80_total minutes"] - 707.2) < 1e-9
    assert all(
        3.0 <= float(row["80_Total day charge"]) <= 60.0
        for row in prepared.select("80_Total day charge").collect()
    )


def test_prepare_inclusive_numeric_filter_and_null_handling(spark):
    base = joined_fixture(spark).first().asDict()
    rows = []
    for value in ["2.99", "3.0", "60.0", "60.01", None, "abc"]:
        row = dict(base)
        row["80_Total day charge"] = value
        rows.append(row)
    synthetic = spark.createDataFrame(rows, schema=JOINED_SCHEMA)
    result = prepare_joined(synthetic)
    assert result.count() == 2
    assert {row["80_Total day charge"] for row in result.collect()} == {"3.0", "60.0"}
