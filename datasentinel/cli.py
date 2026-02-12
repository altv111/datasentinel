import sys

from pyspark.sql import SparkSession
from datasentinel.orchestrator import Orchestrator


def run(config_path: str) -> None:
    """
    Programmatic entry point to execute a DataSentinel config.
    """

    spark = (
        SparkSession.builder
        .appName("DataAssertion")
        .getOrCreate()
    )

    comparator = Orchestrator(spark, config_path)
    comparator.execute_steps()

    spark.stop()


def main(argv=None):
    """
    CLI entry point.
    """
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("Usage: datasentinel <yaml_config_path>")
        sys.exit(1)

    run(args[0])


if __name__ == "__main__":
    main()
