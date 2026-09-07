from pathlib import Path

from churn_pipeline.io import DATASET_DIRS
from churn_pipeline.tasks import join_task, prepare_task, split_task


def test_pipeline_end_to_end(spark, tmp_path):
    base = str(tmp_path / "pipeline")
    common = ["--output_format", "parquet", "--base_output_path", base]
    join_task.main(common)
    prepare_task.main(common)
    split_task.main(common)

    assert Path(base, DATASET_DIRS["train"]).exists()
    assert Path(base, DATASET_DIRS["test"]).exists()
    train = spark.read.parquet(str(Path(base, DATASET_DIRS["train"])))
    test = spark.read.parquet(str(Path(base, DATASET_DIRS["test"])))
    assert train.count() + test.count() == 13_184
