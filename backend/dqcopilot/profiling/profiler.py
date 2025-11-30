"""
Profiling utilities for dq_profiles.

Contains functions to compute per column statistics
(row_count, null_fraction, distinct_count, etc.).
"""

from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..config import DQConfig


@dataclass
class ColumnProfile:
    table_name: str
    column_name: str
    batch_date: str
    row_count: int
    null_count: int
    null_fraction: float
    distinct_count: int
    min_value: str
    max_value: str


def describe_table_schema(
    spark: SparkSession,
    table_name: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> List[dict]:
    """
    Return a simple schema description for a table
    so the agent or tests can inspect it.
    """
    cfg = DQConfig()
    catalog = catalog or cfg.catalog
    schema = schema or cfg.schema

    full_name = f"{catalog}.{schema}.{table_name}"
    df = spark.table(full_name)

    return [
        {
            "name": f.name,
            "type": f.dataType.simpleString(),
            "nullable": f.nullable,
        }
        for f in df.schema.fields
    ]


def profile_table(
    spark: SparkSession,
    table_name: str,
    date_col: str = "created_date",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> Optional[DataFrame]:
    """
    Compute per column profiles for a table.

    Returns a DataFrame with schema matching dq_profiles:
      table_name, column_name, batch_date,
      row_count, null_count, null_fraction,
      distinct_count, min_value, max_value, profile_ts

    Does not write into dq_profiles. Caller is responsible for writing.
    """
    cfg = DQConfig()
    catalog = catalog or cfg.catalog
    schema = schema or cfg.schema

    full_name = f"{catalog}.{schema}.{table_name}"
    df = spark.table(full_name)

    if date_col not in df.columns:
        raise ValueError(
            f"date_col '{date_col}' not found in table {full_name}. "
            f"Available columns: {df.columns}"
        )

    # Normalize to batch_date
    df = df.withColumn("batch_date", F.col(date_col).cast("date"))

    # Numeric vs other columns, similar to the notebook
    numeric_cols = [
        f.name
        for f in df.schema.fields
        if isinstance(
            f.dataType,
            (
                T.IntegerType,
                T.LongType,
                T.DoubleType,
                T.FloatType,
                T.DecimalType,
            ),
        )
    ]
    other_cols = [
        f.name
        for f in df.schema.fields
        if f.name not in numeric_cols and f.name not in ["batch_date"]
    ]

    profile_dfs = []

    for col in numeric_cols + other_cols:
        col_df = (
            df.groupBy("batch_date")
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                F.count(F.when(F.col(col).isNull(), 1)).alias("null_count"),
                F.countDistinct(F.col(col)).alias("distinct_count"),
                F.min(F.col(col)).cast("string").alias("min_value"),
                F.max(F.col(col)).cast("string").alias("max_value"),
            )
            .withColumn("null_fraction", F.col("null_count") / F.col("row_count"))
            .withColumn("table_name", F.lit(table_name))
            .withColumn("column_name", F.lit(col))
            .withColumn("profile_ts", F.current_timestamp())
            .select(
                "table_name",
                "column_name",
                "batch_date",
                "row_count",
                "null_count",
                "null_fraction",
                "distinct_count",
                "min_value",
                "max_value",
                "profile_ts",
            )
        )
        profile_dfs.append(col_df)

    if not profile_dfs:
        return None

    combined = profile_dfs[0]
    for p in profile_dfs[1:]:
        combined = combined.unionByName(p)

    return combined


def compute_profiles_for_table(
    spark: SparkSession,
    table_name: str,
    date_col: str = "created_date",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    truncate_existing: bool = False,
) -> int:
    """
    Convenience function: profile a table and write the rows into dq_profiles.

    Returns the number of profile rows written.
    """
    cfg = DQConfig()
    catalog = catalog or cfg.catalog
    schema = schema or cfg.schema

    profiles_df = profile_table(
        spark=spark,
        table_name=table_name,
        date_col=date_col,
        catalog=catalog,
        schema=schema,
    )

    if profiles_df is None:
        return 0

    dq_profiles_table = f"{catalog}.{schema}.dq_profiles"

    if truncate_existing:
        spark.sql(f"TRUNCATE TABLE {dq_profiles_table}")

    # Write profiles
    profiles_df.write.mode("append").format("delta").saveAsTable(dq_profiles_table)

    # For synthetic scale this count is acceptable
    return profiles_df.count()
