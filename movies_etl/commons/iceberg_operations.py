from typing import Any, Sequence
from pydantic import BaseModel
from pyspark.sql import SparkSession

from movies_etl.commons.helpers import (
    generate_iceberg_create_table_ddl,
    generate_iceberg_merge_into_sql,
    map_pydantic_to_iceberg_schema,
)
from movies_etl.commons.logger import get_logger

logger = get_logger(__name__)


def create_table(
    spark: SparkSession,
    table_name: str,
    model: type[BaseModel] | None = None,
    schema: dict[str, str] | None = None,
    partition_cols: Sequence[str] | None = None,
    location: str | None = None,
    tblproperties: dict[str, str] | None = None,
    if_not_exists: bool = True,
) -> str:
    """Creates an Iceberg table if it does not exist using a Pydantic model or schema mapping.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified table name (e.g., 'spark_catalog.db.titles').
        model: Optional Pydantic BaseModel class from which schema is derived.
        schema: Optional column name -> Iceberg data type dict (used if model is not provided).
        partition_cols: Optional list of partition column names.
        location: Optional custom storage location (e.g. S3 / HDFS URI).
        tblproperties: Optional dictionary of Iceberg table properties.
        if_not_exists: Whether to include 'IF NOT EXISTS' in the DDL statement (default: True).

    Returns:
        The executed DDL query string.
    """
    if model is not None:
        ddl = generate_iceberg_create_table_ddl(
            table_name=table_name,
            model=model,
            if_not_exists=if_not_exists,
            partition_cols=list(partition_cols) if partition_cols else None,
            location=location,
            tblproperties=tblproperties,
        )
    elif schema is not None:
        cols_ddl = ",\n  ".join(f"{col} {dtype}" for col, dtype in schema.items())
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        partition_clause = f"\nPARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
        location_clause = f"\nLOCATION '{location}'" if location else ""
        if tblproperties:
            props_str = ", ".join(f"'{k}'='{v}'" for k, v in tblproperties.items())
            tblprops_clause = f"\nTBLPROPERTIES ({props_str})"
        else:
            tblprops_clause = ""
        ddl = f"CREATE TABLE {exists_clause}{table_name} (\n  {cols_ddl}\n)\nUSING iceberg{partition_clause}{location_clause}{tblprops_clause}"
    else:
        raise ValueError("Either 'model' or 'schema' must be provided to create_table.")

    logger.info("Creating Iceberg table", extra={"table_name": table_name, "ddl": ddl})
    spark.sql(ddl)
    return ddl


def merge_into(
    spark: SparkSession,
    target_table: str,
    source_df: Any,
    join_keys: Sequence[str] | str = "movieid",
    model: type[BaseModel] | None = None,
    columns: Sequence[str] | None = None,
    matched_condition: str | None = "source.op = 'U'",
    not_matched_condition: str | None = None,
    source_view: str = "source_view",
) -> str:
    """Executes a MERGE INTO operation on an Iceberg table using a source DataFrame.

    Args:
        spark: Active SparkSession.
        target_table: Fully qualified table name (e.g., 'spark_catalog.db.titles').
        source_df: Source PySpark DataFrame.
        join_keys: Column name or sequence of column names for matching.
        model: Optional Pydantic BaseModel to derive column list.
        columns: Optional explicit sequence of columns to update/insert.
        matched_condition: Condition for WHEN MATCHED clause (default: "source.op = 'U'").
        not_matched_condition: Condition for WHEN NOT MATCHED clause.
        source_view: Name of the temporary view to register source_df.

    Returns:
        The executed MERGE INTO SQL statement.
    """
    if columns is not None:
        target_columns = list(columns)
    elif model is not None:
        target_columns = list(map_pydantic_to_iceberg_schema(model).keys())
    else:
        target_columns = [col for col in source_df.columns if col not in ("op", "transaction_ts")]

    keys = [join_keys] if isinstance(join_keys, str) else list(join_keys)

    source_df.createOrReplaceTempView(source_view)

    sql = generate_iceberg_merge_into_sql(
        target_table=target_table,
        source_view=source_view,
        join_keys=keys,
        columns=target_columns,
        matched_condition=matched_condition,
        not_matched_condition=not_matched_condition,
    )

    logger.info("Executing MERGE INTO", extra={"target_table": target_table, "sql": sql})
    spark.sql(sql)
    return sql
