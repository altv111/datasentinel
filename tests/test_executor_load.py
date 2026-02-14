from unittest.mock import Mock

from datasentinel.executor import LoadExecutor


def test_load_executor_routes_jdbc_config(monkeypatch):
    spark = Mock()
    path_resolver = Mock()
    df = Mock()

    jdbc_loader = Mock(return_value=df)
    file_loader = Mock()
    monkeypatch.setattr("datasentinel.executor.load_table_data", jdbc_loader)
    monkeypatch.setattr("datasentinel.executor.load_data", file_loader)

    config = {
        "name": "trades",
        "type": "load",
        "format": "jdbc",
        "db_type": "postgres",
        "connection_string": "jdbc:postgresql://localhost:5432/db",
        "query": "SELECT * FROM trade_pricing WHERE tradebook = 'X'",
    }
    out = LoadExecutor(spark, config, path_resolver, run_id="rid").execute()

    assert out is df
    jdbc_loader.assert_called_once()
    file_loader.assert_not_called()
    path_resolver.resolve_input.assert_not_called()
    df.createOrReplaceTempView.assert_called_once_with("trades")


def test_load_executor_routes_file_config(monkeypatch):
    spark = Mock()
    path_resolver = Mock()
    path_resolver.resolve_input.return_value = "/tmp/data.csv"
    df = Mock()

    jdbc_loader = Mock()
    file_loader = Mock(return_value=df)
    monkeypatch.setattr("datasentinel.executor.load_table_data", jdbc_loader)
    monkeypatch.setattr("datasentinel.executor.load_data", file_loader)

    config = {
        "name": "datasetA",
        "type": "load",
        "format": "csv",
        "path": "data.csv",
    }
    out = LoadExecutor(spark, config, path_resolver, run_id="rid").execute()

    assert out is df
    path_resolver.resolve_input.assert_called_once_with("data.csv", allow_cwd_fallback=True)
    file_loader.assert_called_once_with("/tmp/data.csv", "csv", spark)
    jdbc_loader.assert_not_called()
    df.createOrReplaceTempView.assert_called_once_with("datasetA")


def test_load_executor_routes_http_config(monkeypatch):
    spark = Mock()
    path_resolver = Mock()
    df = Mock()

    jdbc_loader = Mock()
    file_loader = Mock()
    http_loader = Mock(return_value=df)
    monkeypatch.setattr("datasentinel.executor.load_table_data", jdbc_loader)
    monkeypatch.setattr("datasentinel.executor.load_data", file_loader)
    monkeypatch.setattr("datasentinel.executor.load_http_data", http_loader)

    config = {
        "name": "api_trades",
        "type": "load",
        "format": "http",
        "url": "https://api.example.com/trades",
        "response_format": "json",
        "params": {"date": "2026-02-12"},
        "headers": {"Authorization": "Bearer token"},
    }

    out = LoadExecutor(spark, config, path_resolver, run_id="rid").execute()

    assert out is df
    http_loader.assert_called_once()
    jdbc_loader.assert_not_called()
    file_loader.assert_not_called()
    path_resolver.resolve_input.assert_not_called()
    df.createOrReplaceTempView.assert_called_once_with("api_trades")
