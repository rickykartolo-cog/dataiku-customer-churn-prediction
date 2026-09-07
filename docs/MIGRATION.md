# Dataiku to Databricks migration

The original DSS flow reads two uploaded CSV datasets, joins them, computes two
aggregate columns, filters invalid day charges, and randomly splits the result.
The migration keeps source fields as strings until the two explicit aggregate
casts, matching the DSS dataset schemas.

## Recipe and dataset mapping

| DSS recipe | DSS input dataset(s) | PySpark task | Output dataset |
| --- | --- | --- | --- |
| `compute_churn_bigml_80_joined` | `churn_bigml_80`, `churn_bigml_20` | `churn_pipeline.tasks.join_task` | `<base_output_path>/churn_bigml_80_joined` |
| `compute_churn_bigml_80_joined_prepared` | `churn_bigml_80_joined` | `churn_pipeline.tasks.prepare_task` | `<base_output_path>/churn_bigml_80_joined_prepared` |
| `split_churn_bigml_80_joined_prepared` | `churn_bigml_80_joined_prepared` | `churn_pipeline.tasks.split_task` | `<base_output_path>/churn_data_train`, `<base_output_path>/churn_data_test` |

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

## Running on Databricks

Validate and deploy the Asset Bundle, then run the whole DAG:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run churn_flow -t dev
```

You can override job parameters when running the bundle, for example:

```bash
databricks bundle run churn_flow -t dev \
  --params base_output_path=/Volumes/<catalog>/<schema>/churn,output_format=delta
```

`databricks bundle run churn_flow --only split` is not supported for rerunning
one task. For debugging, use the deployed job ID with `jobs run-now` and an
`only` task selection (upstream outputs must already exist):

```bash
databricks jobs run-now <job_id> \
  --json '{"only": ["prepare"]}'
```

Alternatively, run a task file directly from a notebook or cluster:

```python
%sh python /Workspace/<bundle root>/files/churn_pipeline/tasks/prepare_task.py \
  --base_output_path /Volumes/<catalog>/<schema>/churn \
  --output_format delta
```

On Databricks, a scheme-less path such as `/tmp/churn_pipeline` resolves to
`dbfs:/tmp/churn_pipeline`, so intermediate outputs persist between tasks. On
a local machine, the same path refers to the local filesystem.

## Verification checklist

| Check | Local verification | Workspace verification |
| --- | --- | --- |
| Tasks run in order: `join` → `prepare` → `split` | End-to-end pytest invokes the three task entrypoints in order | Run the deployed `churn_flow` DAG |
| Join writes 40 prefixed columns and 13,229 rows | Verified by `test_join.py` | Confirm the deployed join task output |
| Prepare writes 42 columns with the two derived columns | Verified by `test_prepare.py` | Confirm the deployed prepare task schema |
| Charge filter leaves 13,184 rows | Verified by `test_prepare.py` | Confirm the deployed prepared dataset count |
| Seed-1337 split writes 10,584 train and 2,600 test rows (about 80.3% / 19.7%) | Verified by the local task run and end-to-end pytest | Confirm counts in the deployed job outputs |
| Final outputs are `churn_data_train` and `churn_data_test` | Verified by `test_pipeline_end_to_end.py` | Confirm the dataset directories in the configured workspace path |

The local checks above were run with pytest and local PySpark. Bundle
validation, deployment, Databricks task scheduling, DBFS/Volume path
resolution, and a workspace execution require Databricks credentials and a
workspace run.
