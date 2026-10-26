from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

from pydantic import BaseModel
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType as SparkDecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from movies_etl.commons.helpers import (
    generate_iceberg_create_table_ddl,
    generate_iceberg_merge_into_sql,
    get_iceberg_type_from_property,
    get_spark_ddl_type_from_property,
    get_spark_struct_type_from_property,
    map_pydantic_to_iceberg_schema,
    map_pydantic_to_spark_ddl_schema,
    map_pydantic_to_spark_struct,
)
from movies_etl.commons.iceberg_operations import create_table, merge_into
from movies_etl.db.ldg.titles import Titles
from movies_etl.wrapper_movies_ldg import load_to_ldg


class ExtendedModel(BaseModel):
    id: int
    title: str
    is_active: bool
    rating: float
    created_at: datetime
    release_date: date
    uid: UUID
    payload: bytes
    price: Decimal
    tags: list[str]
    metadata: dict[str, str]
    optional_note: str | None
    optional_ts: datetime | None


def test_titles_model_mappings():
    iceberg_schema = map_pydantic_to_iceberg_schema(Titles)
    assert "poster_link" not in iceberg_schema
    assert iceberg_schema["votes_count"] == "int"
    assert iceberg_schema["released_year"] == "int"
    assert iceberg_schema["imdb_rating"] == "double"
    assert iceberg_schema["certificate"] == "string"

    spark_ddl_schema = map_pydantic_to_spark_ddl_schema(Titles)
    assert spark_ddl_schema["released_year"] == "int"
    assert spark_ddl_schema["imdb_rating"] == "double"

    spark_struct = map_pydantic_to_spark_struct(Titles)
    assert isinstance(spark_struct["certificate"].dataType, StringType)
    assert spark_struct["certificate"].nullable is True
    assert spark_struct["released_year"].nullable is False


def test_extended_types_mapping():
    iceberg_schema = map_pydantic_to_iceberg_schema(ExtendedModel)
    assert iceberg_schema["created_at"] == "timestamp"
    assert iceberg_schema["release_date"] == "date"
    assert iceberg_schema["uid"] == "uuid"
    assert iceberg_schema["payload"] == "binary"
    assert iceberg_schema["price"] == "decimal(18, 2)"
    assert iceberg_schema["tags"] == "list<string>"
    assert iceberg_schema["metadata"] == "map<string, string>"
    assert iceberg_schema["optional_ts"] == "timestamp"

    spark_ddl_schema = map_pydantic_to_spark_ddl_schema(ExtendedModel)
    assert spark_ddl_schema["created_at"] == "timestamp"
    assert spark_ddl_schema["release_date"] == "date"
    assert spark_ddl_schema["uid"] == "string"
    assert spark_ddl_schema["price"] == "decimal(18, 2)"
    assert spark_ddl_schema["tags"] == "array<string>"
    assert spark_ddl_schema["metadata"] == "map<string, string>"

    spark_struct = map_pydantic_to_spark_struct(ExtendedModel)
    assert isinstance(spark_struct["created_at"].dataType, TimestampType)
    assert isinstance(spark_struct["release_date"].dataType, DateType)
    assert isinstance(spark_struct["tags"].dataType, ArrayType)
    assert isinstance(spark_struct["tags"].dataType.elementType, StringType)
    assert isinstance(spark_struct["metadata"].dataType, MapType)
    assert spark_struct["optional_ts"].nullable is True
    assert spark_struct["created_at"].nullable is False


def test_iceberg_ddl_generation():
    ddl = generate_iceberg_create_table_ddl(
        table_name="spark_catalog.default.titles",
        model=Titles,
        if_not_exists=True,
        partition_cols=["released_year"],
        tblproperties={"write.format.default": "parquet"},
    )
    assert "CREATE TABLE IF NOT EXISTS spark_catalog.default.titles" in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (released_year)" in ddl
    assert "TBLPROPERTIES ('write.format.default'='parquet')" in ddl


def test_create_table_with_mock_spark():
    mock_spark = MagicMock()
    ddl = create_table(
        spark=mock_spark,
        table_name="spark_catalog.default.titles",
        model=Titles,
        partition_cols=["released_year"],
    )
    mock_spark.sql.assert_called_once_with(ddl)
    assert "CREATE TABLE IF NOT EXISTS spark_catalog.default.titles" in ddl


def test_iceberg_merge_into_sql_generation():
    sql = generate_iceberg_merge_into_sql(
        target_table="titles",
        source_view="source_titles",
        join_keys=["movieid"],
        columns=["movieid", "series_title", "released_year"],
        matched_condition="source.op = 'U'",
    )
    assert "MERGE INTO titles AS target" in sql
    assert "USING source_titles AS source" in sql
    assert "ON target.movieid = source.movieid" in sql
    assert "WHEN MATCHED AND source.op = 'U' THEN UPDATE SET" in sql
    assert "target.series_title = source.series_title" in sql
    assert "target.released_year = source.released_year" in sql
    assert "WHEN NOT MATCHED THEN INSERT (" in sql
    assert "movieid" in sql
    assert "source.movieid" in sql


def test_merge_into_with_mock_spark():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    sql = merge_into(
        spark=mock_spark,
        target_table="titles",
        source_df=mock_df,
        join_keys=["movieid"],
        model=Titles,
        matched_condition="source.op = 'U'",
        source_view="source_titles",
    )
    mock_df.createOrReplaceTempView.assert_called_once_with("source_titles")
    mock_spark.sql.assert_called_once_with(sql)
    assert "MERGE INTO titles AS target" in sql
    assert "ON target.movieid = source.movieid" in sql
    assert "WHEN MATCHED AND source.op = 'U' THEN UPDATE SET" in sql


def test_load_to_ldg():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 10
    mock_df.columns = ["movieid", "series_title", "released_year", "certificate", "runtime", "genre", "imdb_rating", "overview", "meta_score", "director", "votes_count", "gross", "op", "transaction_ts"]
    mock_spark.read.csv.return_value = mock_df

    merge_sql = load_to_ldg(mock_spark)
    assert merge_sql is not None
    assert "MERGE INTO titles AS target" in merge_sql
    assert "ON target.movieid = source.movieid" in merge_sql
    assert "WHEN MATCHED AND source.op = 'U' THEN UPDATE SET" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT (" in merge_sql
