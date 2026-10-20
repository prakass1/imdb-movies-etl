from pathlib import Path

from pyspark.sql import SparkSession

from movies_etl.commons.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration mapping
#
# Each entry maps a fully-qualified STG target table name to:
#   sql_file          - path to the CTE query (relative to this file's package root)
#   catalog_db_table  - the fully-qualified landing table that replaces the
#                       {catalog_db_table} placeholder at runtime
# ---------------------------------------------------------------------------
STG_TABLE_CONFIG: dict[str, dict[str, str]] = {
    "spark_catalog.db.s_movies_genres": {
        "sql_file": "db/stg/s_movies_genres.sql",
        "catalog_db_table": "spark_catalog.db.titles",
    },
    "spark_catalog.db.s_top_movies_genre": {
        "sql_file": "db/stg/s_top_movies_genre.sql",
        "catalog_db_table": "spark_catalog.db.titles",
    },
}

_PACKAGE_ROOT = Path(__file__).resolve().parent


def _load_sql(sql_file: str, catalog_db_table: str) -> str:
    """Read a SQL file and substitute the {catalog_db_table} placeholder."""
    sql_path = _PACKAGE_ROOT / sql_file
    raw = sql_path.read_text(encoding="utf-8")
    return raw.format(catalog_db_table=catalog_db_table)


def run_stg(spark: SparkSession, target_table: str) -> str:
    """Execute a CTAS operation for a single STG table.

    Reads the configured CTE query, resolves the {catalog_db_table} placeholder,
    and runs:
        CREATE OR REPLACE TABLE <target_table> USING iceberg AS (<query>)

    Args:
        spark: Active SparkSession.
        target_table: Fully-qualified STG table name; must be a key in STG_TABLE_CONFIG.

    Returns:
        The executed CTAS SQL string.

    Raises:
        KeyError: If target_table is not present in STG_TABLE_CONFIG.
    """
    if target_table not in STG_TABLE_CONFIG:
        raise KeyError(
            f"No STG config found for '{target_table}'. "
            f"Available tables: {list(STG_TABLE_CONFIG.keys())}"
        )

    config = STG_TABLE_CONFIG[target_table]
    query = _load_sql(
        sql_file=config["sql_file"],
        catalog_db_table=config["catalog_db_table"],
    )

    ctas_sql = (
        f"CREATE OR REPLACE TABLE {target_table}\nUSING iceberg\nAS (\n{query}\n)"
    )

    logger.info(
        "Running CTAS for STG table",
        extra={"target_table": target_table, "sql": ctas_sql},
    )
    spark.sql(ctas_sql)
    logger.info("CTAS complete", extra={"target_table": target_table})
    return ctas_sql


def run_all_stg(spark: SparkSession) -> list[str]:
    """Execute CTAS for every table registered in STG_TABLE_CONFIG.

    Args:
        spark: Active SparkSession.

    Returns:
        List of executed CTAS SQL strings, in config order.
    """
    results: list[str] = []
    for table_name in STG_TABLE_CONFIG:
        sql = run_stg(spark, target_table=table_name)
        results.append(sql)
    return results
