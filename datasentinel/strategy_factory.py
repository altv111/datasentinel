# strategy_factory.py

from datasentinel.assert_strategy import (
    FullOuterJoinStrategy,
    AssertStrategy,
    SqlAssertStrategy,
    LocalFastReconStrategy,
    ArrowReconStrategy,
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
        if strategy_name in ("localfast_recon",):
            return LocalFastReconStrategy()
        if strategy_name in ("arrow_recon",):
            return ArrowReconStrategy()
        return SqlAssertStrategy()


if __name__ == "__main__":
    # example usage
    pass
