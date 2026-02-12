from unittest.mock import Mock

import pytest

from datasentinel.orchestrator import Orchestrator
from datasentinel.exceptions import RunTestFailure


class _FakeLoadExecutor:
    calls = 0

    def __init__(self, _spark, _step, _path_resolver, _run_id):
        pass

    def execute(self):
        type(self).calls += 1
        return {"status": "PASS", "dataframes": {}}


class _FakeTestPassExecutor:
    calls = 0

    def __init__(self, _spark, _step, _path_resolver, _run_id):
        pass

    def execute(self):
        type(self).calls += 1
        return {"status": "PASS", "dataframes": {}}


class _FakeTestFailExecutor:
    calls = 0

    def __init__(self, _spark, _step, _path_resolver, _run_id):
        pass

    def execute(self):
        type(self).calls += 1
        return {"status": "FAIL", "dataframes": {}}


def test_orchestrator_execute_steps_no_failures(monkeypatch):
    _FakeLoadExecutor.calls = 0
    _FakeTestPassExecutor.calls = 0
    monkeypatch.setattr(
        "datasentinel.orchestrator.load_config",
        lambda _path: {
            "steps": [
                {"name": "load_a", "type": "load"},
                {"name": "test_a", "type": "test"},
            ]
        },
    )
    monkeypatch.setattr(
        "datasentinel.orchestrator.PathResolver.from_env",
        lambda: Mock(),
    )
    monkeypatch.setattr(
        "datasentinel.orchestrator.EXECUTOR_REGISTRY",
        {"load": _FakeLoadExecutor, "test": _FakeTestPassExecutor},
    )

    o = Orchestrator(spark=Mock(), config_path="cfg.yaml")
    o.execute_steps()
    assert _FakeLoadExecutor.calls == 1
    assert _FakeTestPassExecutor.calls == 1


def test_orchestrator_execute_steps_raises_if_any_test_fails(monkeypatch):
    _FakeTestPassExecutor.calls = 0
    _FakeTestFailExecutor.calls = 0
    monkeypatch.setattr(
        "datasentinel.orchestrator.load_config",
        lambda _path: {
            "steps": [
                {"name": "test_pass", "type": "test"},
                {"name": "test_fail", "type": "test"},
            ]
        },
    )
    monkeypatch.setattr(
        "datasentinel.orchestrator.PathResolver.from_env",
        lambda: Mock(),
    )

    class _SequencedTestExecutor:
        calls = 0

        def __init__(self, _spark, _step, _path_resolver, _run_id):
            pass

        def execute(self):
            type(self).calls += 1
            if type(self).calls == 1:
                return {"status": "PASS", "dataframes": {}}
            return {"status": "FAIL", "dataframes": {}}

    monkeypatch.setattr(
        "datasentinel.orchestrator.EXECUTOR_REGISTRY",
        {"test": _SequencedTestExecutor},
    )

    o = Orchestrator(spark=Mock(), config_path="cfg.yaml")
    with pytest.raises(RunTestFailure, match="One or more tests failed: test_fail"):
        o.execute_steps()
