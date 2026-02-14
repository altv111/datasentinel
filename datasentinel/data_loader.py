from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import csv
import io
import os
import yaml


def load_data(file_path: str, file_format: str, spark: SparkSession) -> DataFrame:
    """
    Loads data into a Spark DataFrame.

    Args:
        file_path: Path to the data file.
        file_format: Format of the data file (e.g., "csv", "parquet", "json", "avro", "xls", "xlsx").
        spark: SparkSession.

    Returns:
        A Spark DataFrame containing the data.
    """
    loader = get_file_loader(file_format)
    return loader.load(file_path, spark)


def load_table_data(
    db_type: str,
    connection_string: str,
    spark: SparkSession,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    jdbc_options: Optional[Dict[str, str]] = None,
    driver: Optional[str] = None,
) -> DataFrame:
    """
    Loads data from JDBC using either a table name or a SQL query.

    Args:
        db_type: Type of the database (e.g., "oracle", "hive", "postgres").
        connection_string: JDBC connection string.
        spark: SparkSession.
        table_name: Name of the table to load.
        query: SQL query to execute and load.
        jdbc_options: Extra JDBC reader options.
        driver: Optional explicit JDBC driver classname (overrides db_type mapping).

    Returns:
        A Spark DataFrame containing the loaded JDBC data.
    """
    if bool(table_name) == bool(query):
        raise ValueError("Provide exactly one of table_name or query for JDBC load.")

    driver_class = driver or get_driver_class(db_type)
    if not driver_class:
        raise ValueError(f"Unsupported database type: {db_type}")

    reader = (
        spark.read.format("jdbc")
        .option("url", connection_string)
        .option("driver", driver_class)
    )
    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", table_name)

    for key, value in (jdbc_options or {}).items():
        reader = reader.option(key, value)

    df = reader.load()
    return df


def _extract_json_path(payload: Any, json_path: Optional[str]) -> Any:
    if not json_path:
        return payload
    current = payload
    for part in json_path.split("."):
        if isinstance(current, dict):
            if part not in current:
                raise ValueError(f"json_path segment '{part}' not found in payload.")
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            idx = int(part)
            if idx < 0 or idx >= len(current):
                raise ValueError(f"json_path index '{part}' out of range.")
            current = current[idx]
            continue
        raise ValueError(f"json_path segment '{part}' is invalid for current payload type.")
    return current


def _to_row_dicts(payload: Any) -> list:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        if not payload:
            return []
        if all(isinstance(item, dict) for item in payload):
            return payload
        return [{"value": item} for item in payload]
    raise ValueError("JSON payload must be an object or list.")


def _empty_csv_dataframe(spark: SparkSession, headers: list) -> DataFrame:
    schema = StructType([StructField(name, StringType(), True) for name in headers])
    return spark.createDataFrame([], schema)


def _http_get(url: str, *, params=None, headers=None, timeout=30):
    import requests

    return requests.get(url, params=params, headers=headers, timeout=timeout)


def load_http_data(
    url: str,
    spark: SparkSession,
    response_format: str,
    method: str = "GET",
    params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: int = 30,
    json_path: Optional[str] = None,
) -> DataFrame:
    """
    Loads data from an HTTP endpoint and materializes it into a Spark DataFrame.
    """
    method_upper = (method or "GET").upper()
    if method_upper != "GET":
        raise ValueError("HTTP loader currently supports only GET.")

    response = _http_get(
        url,
        params=params,
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    fmt = (response_format or "").lower()
    if fmt == "json":
        payload = _extract_json_path(response.json(), json_path)
        rows = _to_row_dicts(payload)
        if not rows:
            raise ValueError("HTTP JSON response produced no rows.")
        return spark.createDataFrame(rows)

    if fmt == "csv":
        reader = csv.DictReader(io.StringIO(response.text))
        rows = list(reader)
        if rows:
            return spark.createDataFrame(rows)
        if reader.fieldnames:
            return _empty_csv_dataframe(spark, reader.fieldnames)
        raise ValueError("HTTP CSV response has no header row.")

    raise ValueError("response_format must be one of: json, csv.")


def load_config(config_path: str) -> dict:
    """Loads the configuration from a YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_driver_class(db_type: str) -> Optional[str]:
    """Returns the driver class name for a given database type."""
    driver_classes = {
        "oracle": "oracle.jdbc.driver.OracleDriver",
        "postgres": "org.postgresql.Driver",
        "hive": "org.apache.hive.jdbc.HiveDriver",  # Example, verify correct classname
    }
    return driver_classes.get(db_type)


class FileLoader(ABC):
    """
    Abstract base class for file loaders.
    """

    @abstractmethod
    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        """
        Loads data into a Spark DataFrame.

        Args:
            file_path: Path to the data file.
            spark: SparkSession.

        Returns:
            A Spark DataFrame containing the data.
        """
        pass


class CsvLoader(FileLoader):
    """
    Loads data from a CSV file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.csv(file_path, header=True, inferSchema=True)


class ParquetLoader(FileLoader):
    """
    Loads data from a Parquet file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.parquet(file_path)


class JsonLoader(FileLoader):
    """
    Loads data from a JSON file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.json(file_path)


class AvroLoader(FileLoader):
    """
    Loads data from an Avro file.
    """

    def load(self, file_path: str, spark: SparkSession) -> DataFrame:
        return spark.read.format("avro").load(file_path)


file_loaders = {
    "csv": CsvLoader(),
    "parquet": ParquetLoader(),
    "json": JsonLoader(),
    "avro": AvroLoader(),
}


def get_file_loader(file_format: str) -> FileLoader:
    """
    Returns the file loader for a given file format.
    """
    loader = file_loaders.get(file_format)
    if not loader:
        raise ValueError(f"Unsupported file format: {file_format}")
    return loader
