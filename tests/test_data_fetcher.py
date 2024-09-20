import unittest
from unittest.mock import patch, MagicMock
from ETL.data_fetcher import DataFetcher
import pandas as pd

class TestDataFetcher(unittest.TestCase):
    @patch('data_fetcher.mt5.copy_rates_range')
    def test_fetch_data_success(self, mock_copy_rates):
        # Mock successful data retrieval
        mock_copy_rates.return_value = [
            (1633036800, 1.1000, 1.1010, 1.0990, 1.1005, 1000),
            (1633036860, 1.1005, 1.1020, 1.1000, 1.1015, 1500)
        ]
        fetcher = DataFetcher()
        symbol = 'EURUSD'
        start_time = pd.Timestamp('2021-10-01 00:00:00').to_pydatetime()
        end_time = pd.Timestamp('2021-10-01 00:10:00').to_pydatetime()

        df = fetcher.fetch_data(symbol, start_time, end_time)

        # Assertions
        self.assertFalse(df.empty)
        self.assertIn('open', df.columns)
        self.assertIn('high', df.columns)
        self.assertIn('low', df.columns)
        self.assertIn('close', df.columns)
        self.assertIn('tick_volume', df.columns)
        self.assertIn('time', df.columns)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.index[0], pd.Timestamp('2021-10-01 00:00:00'))

    @patch('data_fetcher.mt5.copy_rates_range')
    def test_fetch_data_no_data(self, mock_copy_rates):
        # Mock no data returned
        mock_copy_rates.return_value = []
        fetcher = DataFetcher()
        symbol = 'EURUSD'
        start_time = pd.Timestamp('2021-10-01 00:00:00').to_pydatetime()
        end_time = pd.Timestamp('2021-10-01 00:10:00').to_pydatetime()

        df = fetcher.fetch_data(symbol, start_time, end_time)

        # Assertions
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()
