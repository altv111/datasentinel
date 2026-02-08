# strategy_factory.py

from datasentinel.assert_strategy import (
    FullOuterJoinStrategy,
    AssertStrategy,
    SqlAssertStrategy,
)


class StrategyFactory:
    @staticmethod
    def get_assert_strategy(config: dict) -> AssertStrategy:
        """
        Determines the assert strategy based on the configuration.
        """
        strategy_name = config.get("test", config.get("comparison_type", "full_outer_join"))

        if strategy_name in ("full_outer_join", "full_recon"):
            return FullOuterJoinStrategy()
        return SqlAssertStrategy()


if __name__ == "__main__":
    # example usage
    pass
