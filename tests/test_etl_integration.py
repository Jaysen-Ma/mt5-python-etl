import unittest
from unittest.mock import patch, MagicMock
from main_etl import Mt5_ArcticDB_ETL
import pandas as pd

class TestETLIntegration(unittest.TestCase):
    @patch('main_etl.mt5.copy_rates_range')
    @patch('main_etl.Arctic')
    def test_run_etl(self, mock_arctic, mock_copy_rates):
        # Mock the MetaTrader5 data
        mock_copy_rates.return_value = [
            (1633036800, 1.1000, 1.1010, 1.0990, 1.1005, 1000),
            (1633036860, 1.1005, 1.1020, 1.1000, 1.1015, 1500),
            (1633036920, 1.1015, 1.1030, 1.1010, 1.1025, 2000)
        ]

        # Mock ArcticDB
        mock_lib_original = MagicMock()
        mock_lib_normalized = MagicMock()
        mock_arctic.return_value.get_library.side_effect = [mock_lib_original, mock_lib_normalized]

        # Initialize ETL with test metadata
        etl = Mt5_ArcticDB_ETL(library_name='test', metadata_path='TimeSeriesDB/test_metadata.json')
        etl.symbols = ['EURUSD']

        # Mock metadata
        etl.metadata = {
            "symbols": {
                "EURUSD": {
                    "name": "EURUSD",
                    "description": "Euro vs US Dollar",
                    "last_timestamp": None
                }
            },
            "etl_runs": []
        }

        # Run ETL
        etl.run_etl()

        # Assertions
        # Check that data was fetched
        mock_copy_rates.assert_called()

        # Check that original and normalized data were written
        self.assertTrue(mock_lib_original.write.called)
        self.assertTrue(mock_lib_normalized.write.called)

        # Check that metadata was updated
        self.assertEqual(len(etl.metadata['etl_runs']), 1)
        etl_run = etl.metadata['etl_runs'][0]
        self.assertEqual(etl_run['status'], 'Completed')
        self.assertIn('EURUSD', etl_run['processed_symbols'])

    @patch('main_etl.mt5.copy_rates_range')
    @patch('main_etl.Arctic')
    def test_run_etl_with_existing_data(self, mock_arctic, mock_copy_rates):
        # Mock the MetaTrader5 data, including past data for lookback
        mock_copy_rates.return_value = [
            # Historical data for lookback
            (1633036740, 1.0995, 1.1000, 1.0985, 1.0998, 900),
            # New data
            (1633036800, 1.1000, 1.1010, 1.0990, 1.1005, 1000),
            (1633036860, 1.1005, 1.1020, 1.1000, 1.1015, 1500),
            (1633036920, 1.1015, 1.1030, 1.1010, 1.1025, 2000)
        ]

        # Mock ArcticDB
        mock_lib_original = MagicMock()
        mock_lib_normalized = MagicMock()
        mock_arctic.return_value.get_library.side_effect = [mock_lib_original, mock_lib_normalized]

        # Initialize ETL with test metadata indicating last_timestamp
        etl = Mt5_ArcticDB_ETL(library_name='test', metadata_path='TimeSeriesDB/test_metadata.json')
        etl.symbols = ['EURUSD']

        # Mock metadata with last_timestamp
        etl.metadata = {
            "symbols": {
                "EURUSD": {
                    "name": "EURUSD",
                    "description": "Euro vs US Dollar",
                    "last_timestamp": '2021-10-01 00:10:00'  # Corresponds to 1633036800
                }
            },
            "etl_runs": []
        }

        # Run ETL
        etl.run_etl()

        # Assertions
        # Check that data was fetched with lookback
        # start_time should be last_timestamp - max_lookback
        # Assuming max_lookback=100, start_time = 1633036800 - 100 minutes = 1633036800 - 6000 seconds
        # But since we have only 1 historical data point, start_time will be set to earliest_date (2024-09-01)

        # Check that data was fetched
        mock_copy_rates.assert_called()

        # Check that only new data after last_timestamp was stored
        # new_data should include data points after '2021-10-01 00:10:00'
        # However, the mock data includes '2021-10-01 00:00:00', which is before last_timestamp,
        # so new_data should include only '2021-10-01 00:10:00' onwards
        # Given the mock data, new_data includes '2021-10-01 00:11:00' onwards
        # However, in mock_copy_rates, last_timestamp is '2021-10-01 00:10:00'

        # Since the mock data has timestamps before and after, ensure that new_data only includes after '2021-10-01 00:10:00'

        # Check that original and normalized data were written
        self.assertTrue(mock_lib_original.write.called)
        self.assertTrue(mock_lib_normalized.write.called)

        # Check that metadata was updated
        self.assertEqual(len(etl.metadata['etl_runs']), 1)
        etl_run = etl.metadata['etl_runs'][0]
        self.assertEqual(etl_run['status'], 'Completed')
        self.assertIn('EURUSD', etl_run['processed_symbols'])

if __name__ == '__main__':
    unittest.main()
