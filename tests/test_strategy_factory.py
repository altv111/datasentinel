from datasentinel.strategy_factory import StrategyFactory
from datasentinel.assert_strategy import FullOuterJoinStrategy, SqlAssertStrategy


def test_get_assert_strategy_default():
    strategy = StrategyFactory.get_assert_strategy({})
    assert isinstance(strategy, FullOuterJoinStrategy)


def test_get_assert_strategy_unknown():
    strategy = StrategyFactory.get_assert_strategy({"test": "bogus"})
    assert isinstance(strategy, SqlAssertStrategy)


def test_get_assert_strategy_full_recon_alias():
    strategy = StrategyFactory.get_assert_strategy({"test": "full_recon"})
    assert isinstance(strategy, FullOuterJoinStrategy)
