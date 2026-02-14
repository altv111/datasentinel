import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import DataFrame
from datasentinel.assert_strategy import FullOuterJoinStrategy


class TestFullOuterJoinStrategy(unittest.TestCase):

    def setUp(self):
        """Set up a strategy instance for all tests."""
        self.strategy = FullOuterJoinStrategy()

    @patch('datasentinel.assert_strategy.run_full_outer_join_recon')
    def test_uses_precalculated_counts_when_available(self, mock_run_recon):
        """
        Verify that DataFrame.count() is NOT called if the kernel provides counts.
        """
        # 1. Arrange: Create mock DataFrames with mockable .count() methods
        mock_df_mismatches = MagicMock(spec=DataFrame)
        mock_df_a_only = MagicMock(spec=DataFrame)
        mock_df_b_only = MagicMock(spec=DataFrame)

        # 2. Arrange: Define the mock kernel's output, including the 'counts' dict
        mock_run_recon.return_value = {
            "status": "FAIL",
            "counts": {
                "mismatches": 10,
                "a_only": 5,
                "b_only": 2
            },
            "dataframes": {
                "mismatches": mock_df_mismatches,
                "a_only": mock_df_a_only,
                "b_only": mock_df_b_only,
            },
        }

        # 3. Act: Run the assertion
        # The input dataframes (df_a, df_b) don't matter here as the kernel is mocked
        result = self.strategy.assert_(
            df_a=MagicMock(spec=DataFrame),
            df_b=MagicMock(spec=DataFrame),
            attributes={
                "join_columns": ["id"],
                "compare_columns": ["value"],
                "summary_with_counts": True
            }
        )

        # 4. Assert: Check that the expensive .count() methods were never called
        mock_df_mismatches.count.assert_not_called()
        mock_df_a_only.count.assert_not_called()
        mock_df_b_only.count.assert_not_called()

        # Assert that the summary was correctly constructed from the pre-calculated counts
        self.assertEqual(result.get("summary"), "mismatches=10, a_only=5, b_only=2")

    @patch('datasentinel.assert_strategy.run_full_outer_join_recon')
    def test_falls_back_to_count_when_counts_not_available(self, mock_run_recon):
        """
        Verify that DataFrame.count() IS called as a fallback if the kernel
        does not provide a 'counts' dictionary.
        """
        # 1. Arrange: Create mock DataFrames and configure their .count() to return values
        mock_df_mismatches = MagicMock(spec=DataFrame)
        mock_df_mismatches.count.return_value = 10
        mock_df_a_only = MagicMock(spec=DataFrame)
        mock_df_a_only.count.return_value = 5
        mock_df_b_only = MagicMock(spec=DataFrame)
        mock_df_b_only.count.return_value = 2

        # 2. Arrange: Define the mock kernel's output *without* the 'counts' dict
        mock_run_recon.return_value = {
            "status": "FAIL",
            "dataframes": {
                "mismatches": mock_df_mismatches,
                "a_only": mock_df_a_only,
                "b_only": mock_df_b_only,
            },
        }

        # 3. Act: Run the assertion
        result = self.strategy.assert_(
            df_a=MagicMock(spec=DataFrame),
            df_b=MagicMock(spec=DataFrame),
            attributes={
                "join_columns": ["id"],
                "compare_columns": ["value"],
                "summary_with_counts": True
            }
        )

        # 4. Assert: Check that fallback count() is used for all output buckets
        mock_df_mismatches.count.assert_called_once()
        mock_df_a_only.count.assert_called_once()
        mock_df_b_only.count.assert_called_once()
        self.assertEqual(result.get("summary"), "mismatches=10, a_only=5, b_only=2")
