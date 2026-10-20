from movies_etl.commons import spark_session
from movies_etl import wrapper_movies_ldg
from movies_etl.commons.logger import get_logger

logger = get_logger(__name__)
spark = spark_session.get_spark_session(CATALOG_NAME="spark_catalog")

# Ensure the target namespace exists in the REST catalog before creating tables.
# The REST catalog maps this to s3://warehouse/db/ automatically.
spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.db")

logger.info("Executing...")
# Simulate load1
wrapper_movies_ldg.load_to_ldg(spark, load_file="LOAD1.csv")
# Simulate LOAD2 update
#wrapper_movies_ldg.load_to_ldg(spark, load_file="LOAD2.csv")
logger.info("Done...")