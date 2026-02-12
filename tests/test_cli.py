import pytest
from unittest.mock import Mock

from datasentinel import cli
from datasentinel.exceptions import RunTestFailure


def test_cli_requires_config_path(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["datasentinel"])  # no config path
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "Usage: datasentinel <yaml_config_path>" in captured.out


def test_cli_main_delegates_to_run(monkeypatch):
    run_mock = Mock()
    monkeypatch.setattr(cli, "run", run_mock)
    cli.main(["examples/assert_test.yaml"])
    run_mock.assert_called_once_with("examples/assert_test.yaml")


def test_cli_main_handles_test_failure_without_traceback(monkeypatch, capsys):
    def _raise(_path):
        raise RunTestFailure("One or more tests failed: recon_trades_multikey")

    monkeypatch.setattr(cli, "run", _raise)
    with pytest.raises(SystemExit) as exc:
        cli.main(["examples/large_recon_multikey_example.yaml"])

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "One or more tests failed: recon_trades_multikey" in captured.out


def test_run_executes_orchestrator_and_stops_spark(monkeypatch):
    spark = Mock()
    builder = Mock()
    builder.appName.return_value = builder
    builder.getOrCreate.return_value = spark
    monkeypatch.setattr(cli.SparkSession, "builder", builder)

    orchestrator_instance = Mock()
    orchestrator_cls = Mock(return_value=orchestrator_instance)
    monkeypatch.setattr(cli, "Orchestrator", orchestrator_cls)

    cli.run("examples/assert_test.yaml")

    builder.appName.assert_called_once_with("DataAssertion")
    builder.getOrCreate.assert_called_once()
    orchestrator_cls.assert_called_once_with(spark, "examples/assert_test.yaml")
    orchestrator_instance.execute_steps.assert_called_once()
    spark.stop.assert_called_once()
