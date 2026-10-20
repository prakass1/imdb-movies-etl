from movies_etl import wrapper_movies_stg
from movies_etl.commons import spark_session
from movies_etl.commons.logger import get_logger

logger = get_logger(__name__)
spark = spark_session.get_spark_session(CATALOG_NAME="spark_catalog")

# Ensure the target namespace exists before creating STG tables.
spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.db")

logger.info("Executing STG job...")
wrapper_movies_stg.run_all_stg(spark)
logger.info("Done.")
