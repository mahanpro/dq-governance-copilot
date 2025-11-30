import json
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from ..config import DQConfig


def _table_names(catalog: Optional[str], schema: Optional[str]) -> dict:
    cfg = DQConfig()
    catalog = catalog or cfg.catalog
    schema = schema or cfg.schema
    base = f"{catalog}.{schema}"
    return {
        "catalog": catalog,
        "schema": schema,
        "expectations": f"{base}.dq_expectations",
        "expectation_metrics": f"{base}.dq_expectation_metrics",
        "transactions": f"{base}.transactions",  # you can generalise later
    }


def evaluate_expectations_for_table(
    spark: SparkSession,
    table_name: str,
    date_col: str = "created_date",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> int:
    """
    Evaluate all expectations in dq_expectations for a given table.

    Supports:
      - NOT_NULL on any column
      - RANGE on numeric column: params_json {"min": ..., "max": ...}
      - VOLUME_MIN on '*ROW_COUNT*': params_json {"min_rows": ...}

    Writes rows into dq_expectation_metrics and returns number of rows written.
    """
    names = _table_names(catalog, schema)
    exp_tbl = names["expectations"]
    metrics_tbl = names["expectation_metrics"]
    full_table_name = f"{names['catalog']}.{names['schema']}.{table_name}"

    tx = spark.table(full_table_name).withColumn(
        "batch_date", F.col(date_col).cast("date")
    )

    exp_df = spark.table(exp_tbl).where(F.col("table_name") == table_name)

    # Serverless: avoid exp_df.rdd.isEmpty()
    if exp_df.limit(1).count() == 0:
        return 0

    metrics_dfs = []

    for row in exp_df.collect():
        eid = row["expectation_id"]
        col = row["column_name"]
        etype = row["expectation_type"]
        params_raw = row["params_json"] or "{}"
        try:
            params = json.loads(params_raw)
        except Exception:
            params = {}

        if etype == "NOT_NULL":
            agg = (
                tx.groupBy("batch_date")
                .agg(
                    F.count(F.lit(1)).alias("rows_checked"),
                    F.count(F.when(F.col(col).isNull(), 1)).alias("rows_violated"),
                )
                .withColumn(
                    "violation_fraction",
                    F.col("rows_violated") / F.col("rows_checked"),
                )
                .withColumn("expectation_id", F.lit(eid))
                .withColumn("table_name", F.lit(table_name))
                .withColumn("column_name", F.lit(col))
                .withColumn("eval_ts", F.current_timestamp())
            )
            metrics_dfs.append(agg)

        elif etype == "RANGE":
            min_val = float(params.get("min", float("-inf")))
            max_val = float(params.get("max", float("inf")))
            agg = (
                tx.groupBy("batch_date")
                .agg(
                    F.count(F.lit(1)).alias("rows_checked"),
                    F.count(
                        F.when(
                            (F.col(col) < F.lit(min_val))
                            | (F.col(col) > F.lit(max_val))
                            | F.col(col).isNull(),
                            1,
                        )
                    ).alias("rows_violated"),
                )
                .withColumn(
                    "violation_fraction",
                    F.col("rows_violated") / F.col("rows_checked"),
                )
                .withColumn("expectation_id", F.lit(eid))
                .withColumn("table_name", F.lit(table_name))
                .withColumn("column_name", F.lit(col))
                .withColumn("eval_ts", F.current_timestamp())
            )
            metrics_dfs.append(agg)

        elif etype == "VOLUME_MIN":
            min_rows = int(params.get("min_rows", 0))
            base = (
                tx.groupBy("batch_date")
                .agg(F.count(F.lit(1)).alias("rows_checked"))
            )
            agg = (
                base.withColumn(
                    "rows_violated",
                    F.when(
                        F.col("rows_checked") < F.lit(min_rows),
                        F.col("rows_checked"),
                    ).otherwise(F.lit(0)),
                )
                .withColumn(
                    "violation_fraction",
                    F.col("rows_violated") / F.col("rows_checked"),
                )
                .withColumn("expectation_id", F.lit(eid))
                .withColumn("table_name", F.lit(table_name))
                .withColumn("column_name", F.lit(col))
                .withColumn("eval_ts", F.current_timestamp())
            )
            metrics_dfs.append(agg)

        else:
            # Unknown expectation type for now
            continue

    if not metrics_dfs:
        return 0

    combined = metrics_dfs[0]
    for df_part in metrics_dfs[1:]:
        combined = combined.unionByName(df_part)

    combined.select(
        "expectation_id",
        "table_name",
        "column_name",
        "batch_date",
        "rows_checked",
        "rows_violated",
        "violation_fraction",
        "eval_ts",
    ).write.mode("append").format("delta").saveAsTable(metrics_tbl)

    return combined.count()
