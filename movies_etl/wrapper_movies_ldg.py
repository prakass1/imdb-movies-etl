from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField
from movies_etl.commons.iceberg_operations import create_table, merge_into
from movies_etl.commons.logger import get_logger
from movies_etl.db.ldg.titles import Titles
from movies_etl.commons.helpers import map_pydantic_to_spark_struct

logger = get_logger(__name__)


def load_to_ldg(spark: SparkSession, load_file: str):
    # Simulate the seed for reading files from cdc event
    cdc_events = Path(__file__).resolve().parent / "cdc_events"
    schema = map_pydantic_to_spark_struct(model=Titles).add(StructField('op', StringType()))
    if cdc_events.exists():
        df = spark.read.csv(
            f"{cdc_events}/{load_file}",
            header=True,
            nullValue="NULL",
            quote='"',
            escape='"',
            multiLine=True,
            #mode="FAILFAST",
            timestampFormat="yyyy-MM-dd HH:mm:ss.SSSSSSX",
            schema=schema
        )
        logger.info("Loaded CDC files", extra={"files": f"{cdc_events}/{load_file}", "rows": df.count(), "columns": df.columns})
        logger.info("Creating table next ...")
        ddl = create_table(
            spark=spark,
            table_name="spark_catalog.db.titles",  # fully qualified: <catalog>.<database>.<table>
            model=Titles,
            tblproperties={"external.table.purge": "true"},
            # REST catalog derives the S3 path automatically from the warehouse root;
            # no explicit location needed.
        )
        logger.info("Created table as: %s", ddl)

        logger.info("Performing MERGE INTO next ...")
        merge_sql = merge_into(
            spark=spark,
            target_table="spark_catalog.db.titles",
            source_df=df,
            join_keys=["movieid"],
            model=Titles,
            matched_condition="source.op = 'U'",
            not_matched_condition="source.op = 'I'",
            source_view="source_titles",
        )
        logger.info("Merged CDC events into titles table as: %s", merge_sql)
        return merge_sql
    else:
        logger.warning("CDC events directory not found", extra={"path": str(cdc_events)})
        return None
