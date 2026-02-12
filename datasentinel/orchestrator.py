# orchestrator.py

from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timezone
import uuid

from datasentinel.data_loader import load_config
from datasentinel.paths import PathResolver
from datasentinel.executor import (
    LoadExecutor,
    TransformExecutor,
    TesterExecutor,
    WriteExecutor,
)
from datasentinel.exceptions import RunTestFailure

EXECUTOR_REGISTRY = {
    "load": LoadExecutor,
    "transform": TransformExecutor,
    "test": TesterExecutor,
}


class Orchestrator:
    """
    Main class for comparing two datasets.
    """

    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.path_resolver = PathResolver.from_env()
        self.run_id = self._generate_run_id()
        self.config = load_config(config_path)

    def execute_steps(self):
        """
        Executes the steps defined in the configuration.
        """
        failed_tests = []
        for step in self.config["steps"]:
            try:
                step_type = step["type"]
                executor_cls = EXECUTOR_REGISTRY.get(step_type)
                if executor_cls is None:
                    supported = ", ".join(sorted(EXECUTOR_REGISTRY))
                    raise ValueError(
                        f"Unknown step type '{step_type}' in step "
                        f"'{step.get('name', 'unnamed')}'. "
                        f"Supported types: {supported}"
                    )
                executor = executor_cls(self.spark, step, self.path_resolver, self.run_id)
                result = executor.execute()
                if step_type == "test" and isinstance(result, dict):
                    if result.get("status") == "FAIL":
                        failed_tests.append(step.get("name", "unnamed"))

                if step.get("write", False):
                    dataframes = {}
                    if isinstance(result, DataFrame):
                        dataframes["output"] = result
                    elif isinstance(result, dict):
                        if "dataframes" in result and isinstance(result["dataframes"], dict):
                            dataframes = result["dataframes"]
                        else:
                            dataframes = result
                    WriteExecutor(self.spark, step, dataframes, self.path_resolver, self.run_id).execute()

            except Exception as e:
                print(f"Failed to execute step. See {e}")
                raise
        if failed_tests:
            names = ", ".join(failed_tests)
            raise RunTestFailure(f"One or more tests failed: {names}")

    @staticmethod
    def _generate_run_id() -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"{timestamp}-{uuid.uuid4().hex[:8]}"
