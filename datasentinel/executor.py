# executor.py

from pyspark.sql import SparkSession, DataFrame
import logging
import os

from datasentinel.data_loader import load_data
from datasentinel.strategy_factory import StrategyFactory


class Executor:
    """
    Base class for executors.
    """

    def __init__(self, spark: SparkSession, config: dict, path_resolver, run_id: str):
        self.spark = spark
        self.config = config
        self.path_resolver = path_resolver
        self.run_id = run_id

    def execute(self):
        """
        Executes the logic defined in the subclass.
        Should be implemented in each subclass.
        """
        raise NotImplementedError(
            "Execute method must be implemented in subclass"
        )


class WriteExecutor(Executor):
    def __init__(self, spark: SparkSession, config: dict, dataframes: dict, path_resolver, run_id: str):
        super().__init__(spark, config, path_resolver, run_id)
        self.dataframes = dataframes

    def execute(self):
        write_path = self.config.get("write_path")
        write_format = self.config.get("write_format", "csv")

        step_name = self.config.get("name", "unnamed")
        base_dir = self.path_resolver.output_base_dir()
        target_root = os.path.join(base_dir, self.run_id, step_name)

        for name, df in self.dataframes.items():
            target_path = target_root
            if write_path:
                target_path = os.path.join(target_root, write_path)
            if len(self.dataframes) > 1:
                target_path = os.path.join(target_path, name)
            df.write.mode("overwrite").format(write_format).option("header", "true").save(target_path)


class LoadExecutor(Executor):
    """
    Executor for loading data.
    """

    def execute(self) -> DataFrame:
        """
        Loads data into a Spark DataFrame and registers it as a temporary view.
        """
        try:
            step_name = self.config["name"]
            file_path = self.path_resolver.resolve_input(self.config["path"], allow_cwd_fallback=True)
            file_format = self.config["format"]

            df = load_data(file_path, file_format, self.spark)
            df.createOrReplaceTempView(step_name)

            return df

        except Exception as e:
            logging.error(f"Error in LoadExecutor: {e}")
            raise


class TransformExecutor(Executor):
    """
    Executor for transforming data.
    """

    def execute(self) -> DataFrame:
        """
        Executes a SQL query on a temporary view and registers the result
        as a new temporary view.
        """
        try:
            step_name = self.config["name"]
            query = self.config["query"]

            df = self.spark.sql(query)
            df.createOrReplaceTempView(step_name)

            return df

        except Exception as e:
            logging.error(f"Error in TransformExecutor: {e}")
            raise

class TesterExecutor(Executor):
    def __init__(self, spark: SparkSession, config: dict, path_resolver, run_id: str):
        super().__init__(spark, config, path_resolver, run_id)
        self.strategy = StrategyFactory.get_assert_strategy(config)

    def execute(self) -> dict:
        lhs_name = self.config.get("LHS")
        rhs_name = self.config.get("RHS")
        lhs_query = self.config.get("LHS_query")
        rhs_query = self.config.get("RHS_query")
        if not lhs_name and not lhs_query:
            raise ValueError("Test steps require LHS or LHS_query.")
        if lhs_name and lhs_query:
            raise ValueError("Provide only one of LHS or LHS_query.")
        if rhs_name and rhs_query:
            raise ValueError("Provide only one of RHS or RHS_query.")

        df_a = self.spark.sql(lhs_query) if lhs_query else self.spark.table(lhs_name)
        df_b = self.spark.sql(rhs_query) if rhs_query else (self.spark.table(rhs_name) if rhs_name else None)

        attributes = dict(self.config.get("additional_attributes") or {})
        if "condition_name" not in attributes and self.config.get("test"):
            attributes["condition_name"] = self.config["test"]

        return self.strategy.assert_(df_a, df_b, attributes)
