from datetime import date, datetime
from typing import Any, Type
from uuid import UUID

from pydantic import BaseModel
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Format and primitive mappings for Iceberg and Spark
# ---------------------------------------------------------------------------

_FORMAT_TO_ICEBERG = {
    "date-time": "timestamp",
    "date": "date",
    "time": "time",
    "uuid": "uuid",
    "binary": "binary",
}

_FORMAT_TO_SPARK_DDL = {
    "date-time": "timestamp",
    "date": "date",
    "time": "string",
    "uuid": "string",
    "binary": "binary",
}

_FORMAT_TO_SPARK_TYPE: dict[str, type[DataType]] = {
    "date-time": TimestampType,
    "date": DateType,
    "time": StringType,
    "uuid": StringType,
    "binary": BinaryType,
}

_PRIMITIVE_TO_ICEBERG = {
    "string": "string",
    "integer": "int",
    "int": "int",
    "long": "bigint",
    "bigint": "bigint",
    "number": "double",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "binary": "binary",
    "bytes": "binary",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "date": "date",
}

_PRIMITIVE_TO_SPARK_DDL = {
    "string": "string",
    "integer": "int",
    "int": "int",
    "long": "bigint",
    "bigint": "bigint",
    "number": "double",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "bool": "boolean",
    "binary": "binary",
    "bytes": "binary",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "date": "date",
}

_PRIMITIVE_TO_SPARK_TYPE: dict[str, type[DataType]] = {
    "string": StringType,
    "integer": IntegerType,
    "int": IntegerType,
    "long": LongType,
    "bigint": LongType,
    "number": DoubleType,
    "float": FloatType,
    "double": DoubleType,
    "boolean": BooleanType,
    "bool": BooleanType,
    "binary": BinaryType,
    "bytes": BinaryType,
    "datetime": TimestampType,
    "timestamp": TimestampType,
    "date": DateType,
}


def _unwrap_property(prop: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Extract inner schema property and nullability from JSON Schema property definition."""
    is_nullable = False

    if "anyOf" in prop:
        sub_props = prop["anyOf"]
        non_null_props = [p for p in sub_props if p.get("type") != "null"]
        if any(p.get("type") == "null" for p in sub_props):
            is_nullable = True

        # Special case: Decimal in pydantic json schema is represented as anyOf: [{'type': 'number'}, {'type': 'string'}]
        is_decimal = (
            len(sub_props) == 2
            and {"type": "number"} in sub_props
            and {"type": "string"} in sub_props
        )
        if is_decimal:
            return {"type": "decimal", "precision": 18, "scale": 2}, is_nullable

        if len(non_null_props) == 1:
            return non_null_props[0], is_nullable
        elif len(non_null_props) > 1:
            raise ValueError(f"Union types across multiple non-null types are not supported: {prop}")

    return prop, is_nullable


def get_iceberg_type_from_property(prop: dict[str, Any] | str) -> str:
    """Resolve an Iceberg SQL data type from a JSON schema property dict or type name string."""
    if isinstance(prop, str):
        return _PRIMITIVE_TO_ICEBERG.get(prop, "string")

    prop_clean, _ = _unwrap_property(prop)
    prop_format = prop_clean.get("format")
    prop_type = prop_clean.get("type")

    if prop_format and prop_format in _FORMAT_TO_ICEBERG:
        return _FORMAT_TO_ICEBERG[prop_format]

    if prop_type == "decimal":
        prec = prop_clean.get("precision", 18)
        scale = prop_clean.get("scale", 2)
        return f"decimal({prec}, {scale})"

    if prop_type == "array":
        items = prop_clean.get("items", {})
        item_type = get_iceberg_type_from_property(items)
        return f"list<{item_type}>"

    if prop_type == "object":
        add_props = prop_clean.get("additionalProperties")
        if isinstance(add_props, dict):
            val_type = get_iceberg_type_from_property(add_props)
            return f"map<string, {val_type}>"
        return "map<string, string>"

    return _PRIMITIVE_TO_ICEBERG.get(str(prop_type), "string")


def get_spark_ddl_type_from_property(prop: dict[str, Any] | str) -> str:
    """Resolve a Spark SQL DDL data type string from a JSON schema property dict or type name."""
    if isinstance(prop, str):
        return _PRIMITIVE_TO_SPARK_DDL.get(prop, "string")

    prop_clean, _ = _unwrap_property(prop)
    prop_format = prop_clean.get("format")
    prop_type = prop_clean.get("type")

    if prop_format and prop_format in _FORMAT_TO_SPARK_DDL:
        return _FORMAT_TO_SPARK_DDL[prop_format]

    if prop_type == "decimal":
        prec = prop_clean.get("precision", 18)
        scale = prop_clean.get("scale", 2)
        return f"decimal({prec}, {scale})"

    if prop_type == "array":
        items = prop_clean.get("items", {})
        item_type = get_spark_ddl_type_from_property(items)
        return f"array<{item_type}>"

    if prop_type == "object":
        add_props = prop_clean.get("additionalProperties")
        if isinstance(add_props, dict):
            val_type = get_spark_ddl_type_from_property(add_props)
            return f"map<string, {val_type}>"
        return "map<string, string>"

    return _PRIMITIVE_TO_SPARK_DDL.get(str(prop_type), "string")


def get_spark_struct_type_from_property(prop: dict[str, Any] | str) -> DataType:
    """Resolve a PySpark DataType instance from a JSON schema property dict or type name."""
    if isinstance(prop, str):
        type_cls = _PRIMITIVE_TO_SPARK_TYPE.get(prop, StringType)
        return type_cls()

    prop_clean, _ = _unwrap_property(prop)
    prop_format = prop_clean.get("format")
    prop_type = prop_clean.get("type")

    if prop_format and prop_format in _FORMAT_TO_SPARK_TYPE:
        return _FORMAT_TO_SPARK_TYPE[prop_format]()

    if prop_type == "decimal":
        prec = prop_clean.get("precision", 18)
        scale = prop_clean.get("scale", 2)
        return DecimalType(prec, scale)

    if prop_type == "array":
        items = prop_clean.get("items", {})
        item_type = get_spark_struct_type_from_property(items)
        return ArrayType(item_type)

    if prop_type == "object":
        add_props = prop_clean.get("additionalProperties")
        if isinstance(add_props, dict):
            val_type = get_spark_struct_type_from_property(add_props)
            return MapType(StringType(), val_type)
        return MapType(StringType(), StringType())

    type_cls = _PRIMITIVE_TO_SPARK_TYPE.get(str(prop_type), StringType)
    return type_cls()


# ---------------------------------------------------------------------------
# High-Level Mapping APIs
# ---------------------------------------------------------------------------

def map_pydantic_to_iceberg_schema(model: Type[BaseModel] | Any) -> dict[str, str]:
    """Generates column_name -> Iceberg SQL data type mapping for a Pydantic model."""
    schema = model.model_json_schema()
    result: dict[str, str] = {}
    for key, prop in schema.get("properties", {}).items():
        result[key] = get_iceberg_type_from_property(prop)
    return result


def map_pydantic_to_spark_ddl_schema(model: Type[BaseModel] | Any) -> dict[str, str]:
    """Generates column_name -> Spark SQL DDL data type mapping for a Pydantic model."""
    schema = model.model_json_schema()
    result: dict[str, str] = {}
    for key, prop in schema.get("properties", {}).items():
        result[key] = get_spark_ddl_type_from_property(prop)
    return result


def map_pydantic_to_spark_struct(model: Type[BaseModel] | Any) -> StructType:
    """Generates a PySpark StructType schema with proper types and nullability for a Pydantic model."""
    schema = model.model_json_schema()
    fields: list[StructField] = []
    for key, prop in schema.get("properties", {}).items():
        _, is_nullable = _unwrap_property(prop)
        spark_dtype = get_spark_struct_type_from_property(prop)
        fields.append(StructField(name=key, dataType=spark_dtype, nullable=is_nullable))
    return StructType(fields)


def generate_iceberg_create_table_ddl(
    table_name: str,
    model: Type[BaseModel] | Any,
    if_not_exists: bool = True,
    partition_cols: list[str] | None = None,
    location: str | None = None,
    tblproperties: dict[str, str] | None = None,
) -> str:
    """Generates a CREATE TABLE [IF NOT EXISTS] DDL for Apache Iceberg from a Pydantic model."""
    iceberg_schema = map_pydantic_to_iceberg_schema(model)
    cols_ddl = ",\n  ".join(f"{col} {dtype}" for col, dtype in iceberg_schema.items())
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    partition_clause = f"\nPARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    location_clause = f"\nLOCATION '{location}'" if location else ""
    if tblproperties:
        props_str = ", ".join(f"'{k}'='{v}'" for k, v in tblproperties.items())
        tblprops_clause = f"\nTBLPROPERTIES ({props_str})"
    else:
        tblprops_clause = ""

    return f"CREATE TABLE {exists_clause}{table_name} (\n  {cols_ddl}\n)\nUSING iceberg{partition_clause}{location_clause}{tblprops_clause}"


def generate_iceberg_merge_into_sql(
    target_table: str,
    source_view: str,
    join_keys: list[str] | str,
    columns: list[str],
    matched_condition: str | None = "source.op = 'U'",
    not_matched_condition: str | None = None,
) -> str:
    """Generates an Apache Iceberg MERGE INTO SQL statement.

    Args:
        target_table: Name of the target Iceberg table.
        source_view: Name of the source temporary view or table.
        join_keys: Column name or list of column names used to match records.
        columns: List of columns to update and insert.
        matched_condition: Optional condition for WHEN MATCHED clause (default: "source.op = 'U'").
        not_matched_condition: Optional condition for WHEN NOT MATCHED clause.

    Returns:
        The formatted MERGE INTO SQL query string.
    """
    keys = [join_keys] if isinstance(join_keys, str) else list(join_keys)
    join_clause = " AND ".join(f"target.{k} = source.{k}" for k in keys)
    update_cols = [c for c in columns if c not in keys]
    update_set_clause = ",\n    ".join(f"target.{c} = source.{c}" for c in update_cols)

    matched_cond = f" AND {matched_condition}" if matched_condition else ""
    not_matched_cond = f" AND {not_matched_condition}" if not_matched_condition else ""

    cols_clause = ",\n    ".join(columns)
    vals_clause = ",\n    ".join(f"source.{c}" for c in columns)

    return (
        f"MERGE INTO {target_table} AS target\n"
        f"USING {source_view} AS source\n"
        f"ON {join_clause}\n"
        f"WHEN MATCHED{matched_cond} THEN UPDATE SET\n"
        f"    {update_set_clause}\n"
        f"WHEN NOT MATCHED{not_matched_cond} THEN INSERT (\n"
        f"    {cols_clause}\n"
        f"  ) VALUES (\n"
        f"    {vals_clause}\n"
        f"  )"
    )


# ---------------------------------------------------------------------------
# Backward-compatibility aliases
# ---------------------------------------------------------------------------

def get_iceberg_map_type_from_pydantic(m_type: str) -> str | None:
    """Legacy helper: returns Iceberg type string for a primitive string."""
    return _PRIMITIVE_TO_ICEBERG.get(m_type)


def map_pydantic_to_iceberg_type(model: Type[BaseModel] | Any) -> dict[str, str]:
    """Legacy helper alias for map_pydantic_to_iceberg_schema."""
    return map_pydantic_to_iceberg_schema(model)
