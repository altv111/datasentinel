import pytest
from unittest.mock import Mock

from datasentinel.data_loader import (
    get_driver_class,
    get_file_loader,
    load_config,
    load_http_data,
    load_table_data,
)


def _positional_args(mock_call):
    # Python 3.7 call objects do not expose .args/.kwargs properties.
    return mock_call[0]


def test_get_driver_class_known():
    assert get_driver_class("oracle") == "oracle.jdbc.driver.OracleDriver"
    assert get_driver_class("postgres") == "org.postgresql.Driver"
    assert get_driver_class("hive") == "org.apache.hive.jdbc.HiveDriver"


def test_get_driver_class_unknown_returns_none():
    assert get_driver_class("sqlite") is None


def test_get_file_loader_unknown():
    with pytest.raises(ValueError, match="Unsupported file format"):
        get_file_loader("xls")


def test_load_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("steps:\n  - name: demo\n    type: load\n")
    config = load_config(str(config_file))
    assert config["steps"][0]["name"] == "demo"


def test_load_table_data_uses_query():
    spark = Mock()
    reader = Mock()
    spark.read = reader
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = "df"

    out = load_table_data(
        db_type="postgres",
        connection_string="jdbc:postgresql://localhost:5432/db",
        spark=spark,
        query="SELECT * FROM trade_pricing WHERE tradebook = 'X'",
    )

    assert out == "df"
    calls = [_positional_args(c) for c in reader.option.call_args_list]
    assert ("url", "jdbc:postgresql://localhost:5432/db") in calls
    assert ("driver", "org.postgresql.Driver") in calls
    assert ("query", "SELECT * FROM trade_pricing WHERE tradebook = 'X'") in calls


def test_load_table_data_uses_dbtable_and_extra_options():
    spark = Mock()
    reader = Mock()
    spark.read = reader
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = "df"

    out = load_table_data(
        db_type="oracle",
        connection_string="jdbc:oracle:thin:@//host:1521/service",
        spark=spark,
        table_name="TRADE_PRICING",
        jdbc_options={"fetchsize": "1000"},
    )

    assert out == "df"
    calls = [_positional_args(c) for c in reader.option.call_args_list]
    assert ("dbtable", "TRADE_PRICING") in calls
    assert ("fetchsize", "1000") in calls


def test_load_table_data_requires_exactly_one_source():
    spark = Mock()
    with pytest.raises(ValueError, match="exactly one of table_name or query"):
        load_table_data(
            db_type="postgres",
            connection_string="jdbc:postgresql://localhost:5432/db",
            spark=spark,
            table_name="t",
            query="SELECT * FROM t",
        )
    with pytest.raises(ValueError, match="exactly one of table_name or query"):
        load_table_data(
            db_type="postgres",
            connection_string="jdbc:postgresql://localhost:5432/db",
            spark=spark,
        )


def test_load_http_data_json_with_path(monkeypatch):
    spark = Mock()
    spark.createDataFrame.return_value = "df"

    response = Mock()
    response.json.return_value = {"data": {"items": [{"id": 1, "book": "EQD"}]}}
    response.raise_for_status.return_value = None

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("datasentinel.data_loader._http_get", get_mock)

    out = load_http_data(
        url="https://api.example.com/trades",
        spark=spark,
        response_format="json",
        json_path="data.items",
    )

    assert out == "df"
    get_mock.assert_called_once()
    spark.createDataFrame.assert_called_once_with([{"id": 1, "book": "EQD"}])


def test_load_http_data_csv_and_empty_csv(monkeypatch):
    spark = Mock()
    spark.createDataFrame.side_effect = ["df_nonempty", "df_empty"]

    response_nonempty = Mock()
    response_nonempty.text = "id,book\n1,EQD\n2,FICC\n"
    response_nonempty.raise_for_status.return_value = None
    response_empty = Mock()
    response_empty.text = "id,book\n"
    response_empty.raise_for_status.return_value = None

    get_mock = Mock(side_effect=[response_nonempty, response_empty])
    monkeypatch.setattr("datasentinel.data_loader._http_get", get_mock)

    out_nonempty = load_http_data(
        url="https://api.example.com/trades.csv",
        spark=spark,
        response_format="csv",
    )
    out_empty = load_http_data(
        url="https://api.example.com/trades_empty.csv",
        spark=spark,
        response_format="csv",
    )

    assert out_nonempty == "df_nonempty"
    assert out_empty == "df_empty"


def test_load_http_data_validates_method_and_format(monkeypatch):
    spark = Mock()
    with pytest.raises(ValueError, match="supports only GET"):
        load_http_data(
            url="https://api.example.com/trades",
            spark=spark,
            response_format="json",
            method="POST",
        )

    response = Mock()
    response.raise_for_status.return_value = None
    response.text = ""
    response.json.return_value = {}
    monkeypatch.setattr("datasentinel.data_loader._http_get", Mock(return_value=response))
    with pytest.raises(ValueError, match="response_format must be one of: json, csv"):
        load_http_data(
            url="https://api.example.com/trades",
            spark=spark,
            response_format="xml",
        )
