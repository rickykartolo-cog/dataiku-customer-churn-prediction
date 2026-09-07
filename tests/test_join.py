from pathlib import Path

from churn_pipeline.io import INPUT_COLUMNS, read_source_csv
from churn_pipeline.recipes.join import join_80_20


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_join_shape_and_keys(spark):
    df80 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_80/churn-bigml-80.csv")
    df20 = read_source_csv(spark, REPO_ROOT / "uploads/churn_bigml_20/churn-bigml-20.csv")
    joined = join_80_20(df80, df20)

    assert joined.count() == 13_229
    assert len(joined.columns) == 40
    assert joined.columns == [f"80_{column}" for column in INPUT_COLUMNS] + [
        f"20_{column}" for column in INPUT_COLUMNS
    ]
    assert (
        joined.where(
            (joined["80_State"] != joined["20_State"])
            | (joined["80_Area code"] != joined["20_Area code"])
        ).count()
        == 0
    )
