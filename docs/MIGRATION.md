# Dataiku to Databricks migration

The original DSS flow reads two uploaded CSV datasets, joins them, computes two
aggregate columns, filters invalid day charges, and randomly splits the result.
The migration keeps source fields as strings until the two explicit aggregate
casts, matching the DSS dataset schemas.

## Recipe and dataset mapping

| DSS recipe | DSS input dataset(s) | PySpark task | Output dataset |
| --- | --- | --- | --- |
| `compute_churn_bigml_80_joined` | `churn_bigml_80`, `churn_bigml_20` | `churn_pipeline.tasks.join_task` | `<base_output_path>/joined` |
| `compute_churn_bigml_80_joined_prepared` | `churn_bigml_80_joined` | `churn_pipeline.tasks.prepare_task` | `<base_output_path>/prepared` |
| `split_churn_bigml_80_joined_prepared` | `churn_bigml_80_joined_prepared` | `churn_pipeline.tasks.split_task` | `<base_output_path>/train`, `<base_output_path>/test` |

The `churn_flow` Asset Bundle Job runs these tasks in the same dependency order
on one single-node job cluster. Delta is the default output format; Parquet and
CSV are available for local checks or environments without Delta support.

## DSS step translation

| DSS step | PySpark expression |
| --- | --- |
| Uploaded-file CSV reader (`excel` style, comma, header) | `spark.read.option("header", True).option("sep", ",").option("quote", '"').option("escape", '"').option("inferSchema", False).csv(path)` |
| Join on `State` and `Area code` | `left.join(right, (F.col("80_State") == F.col("20_State")) & (F.col("80_Area code") == F.col("20_Area code")), "inner")` |
| Rename source columns with 80/20 prefixes | `df.select(F.col(column).alias(f"80_{column}") ...)` and the corresponding `20_` expression |
| Shaker `CreateColumnWithGREL`: total minutes | `F.col("80_Total day minutes").cast("double") + F.col("80_Total eve minutes").cast("double") + F.col("80_Total night minutes").cast("double")` |
| Shaker `CreateColumnWithGREL`: total calls | `F.col("80_Total day calls").cast("long") + F.col("80_Total eve calls").cast("long") + F.col("80_Total night calls").cast("long")` |
| Shaker `FilterOnNumericalRange`, keep 3.0 through 60.0, no empty values | `day_charge.isNotNull() & (day_charge >= F.lit(3.0)) & (day_charge <= F.lit(60.0))` |
| Shaker column selection/order | `prepared.select(PREPARED_COLUMNS)` |
| Split recipe random split, seed 1337, train share 80% | `df.coalesce(1).withColumn("_split_rnd", F.rand(1337))`; filter `< 0.8` and `>= 0.8` |

DSS's internal random number generator cannot be reproduced bit-for-bit by
Spark. The split therefore targets parity at the share level, as noted in the
framework's S3 migration guidance; the Spark implementation is deterministic
for a given input order because it coalesces to one partition before calling
`rand(seed)`.

## Local execution

Install the package and test dependencies, then run:

```bash
pip install -e '.[test]'
python -m pytest tests -q
python -m churn_pipeline.tasks.join_task --output_format parquet --base_output_path /tmp/cp_check
python -m churn_pipeline.tasks.prepare_task --output_format parquet --base_output_path /tmp/cp_check
python -m churn_pipeline.tasks.split_task --output_format parquet --base_output_path /tmp/cp_check
```

For a deployed job, set `base_output_path` to a Unity Catalog Volume such as
`/Volumes/<catalog>/<schema>/churn`, or to a DBFS path.
