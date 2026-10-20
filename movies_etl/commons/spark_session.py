import os

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "movies", CATALOG_NAME="spark_catalog") -> SparkSession:
    """Return the active SparkSession configured with an Iceberg REST catalog."""
    catalog = f"spark.sql.catalog.{CATALOG_NAME}"
    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    # Pass the region as a JVM system property on both driver and executors so that
    # the AWS SDK v2 DefaultAwsRegionProviderChain can resolve it. The catalog-scoped
    # s3.region property alone is not sufficient — the SDK reads aws.region at JVM startup
    # before Iceberg initialises the S3FileIO client.
    aws_region_jvm_opt = f"-Daws.region={aws_region}"
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.12.0",
        )
        .config("spark.driver.extraJavaOptions", aws_region_jvm_opt)
        .config("spark.executor.extraJavaOptions", aws_region_jvm_opt)
        # SparkCatalog    – standalone Iceberg catalog; talks directly to the REST server.
        #                   Use this for any catalog name (e.g. "spark_catalog", "iceberg").
        # SparkSessionCatalog – wraps Spark's built-in spark_catalog and delegates to a hidden
        #                       "rest_backend". CREATE NAMESPACE is intercepted by the Spark layer
        #                       but never forwarded, so subsequent CREATE TABLE fails with
        #                       "Namespace does not exist". Do NOT use with a REST catalog.
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        # "rest"    – delegates all warehouse/FileIO config to the REST catalog server (iceberg-rest:8181).
        #             This is the correct type when using MinIO/S3 storage, as the REST server already
        #             has S3FileIO, the bucket endpoint, and path-style access configured.
        # "hadoop"  – resolves paths via Hadoop FileSystem API; cannot handle s3:// without extra config.
        # "hive"    – requires a running Hive Metastore Thrift server (port 9083); not in this stack.
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"))
        .config(f"{catalog}.s3.endpoint", os.environ.get("S3_ENDPOINT", "http://localhost:9000"))
        .config(f"{catalog}.s3.path-style-access", "true")
        # AWS region must be set explicitly so the S3FileIO on each executor can resolve it.
        # The AWS SDK chain (env vars, instance metadata, etc.) is not available when running
        # locally outside Docker — without this, executors fail with "region is required".
        .config(f"{catalog}.s3.region", os.environ.get("AWS_REGION", "us-east-1"))
        .getOrCreate()
    )
