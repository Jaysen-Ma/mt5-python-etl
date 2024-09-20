import unittest
from unittest.mock import patch, MagicMock
from ETL.data_store import DataStore
import pandas as pd

class TestDataStore(unittest.TestCase):
    @patch('data_store.Arctic')
    def test_store_original_data(self, mock_arctic):
        mock_lib = MagicMock()
        mock_arctic.return_value.get_library.return_value = mock_lib

        store = DataStore(library_name='test', normalized=False)
        data = pd.DataFrame({
            'close': [1.1000, 1.1010],
            'SMA_10': [1.1005, 1.1007]
        }, index=pd.date_range(start='2024-09-01', periods=2, freq='T'))

        store.store_data('EURUSD', data)
        mock_lib.write.assert_called_with('EURUSD', data)

    @patch('data_store.Arctic')
    def test_store_normalized_data(self, mock_arctic):
        mock_lib = MagicMock()
        mock_arctic.return_value.get_library.return_value = mock_lib

        store = DataStore(library_name='test', normalized=True)
        data = pd.DataFrame({
            'close': [1.1000, 1.1010],
            'SMA_10': [1.1005, 1.1007]
        }, index=pd.date_range(start='2024-09-01', periods=2, freq='T'))

        normalized_data = pd.DataFrame({
            'close': [0.0, 1.0],
            'SMA_10': [0.0, 1.0]
        }, index=data.index)

        with patch.object(store, 'normalize_data', return_value=normalized_data):
            store.store_data('EURUSD', data)
            mock_lib.write.assert_called_with('EURUSD', normalized_data)

    @patch('data_store.Arctic')
    def test_retrieve_data(self, mock_arctic):
        mock_lib = MagicMock()
        mock_arctic.return_value.get_library.return_value = mock_lib
        expected_data = pd.DataFrame({
            'close': [1.1000, 1.1010],
            'SMA_10': [1.1005, 1.1007]
        }, index=pd.date_range(start='2024-09-01', periods=2, freq='T'))
        mock_lib.read.return_value.data = expected_data

        store = DataStore(library_name='test', normalized=False)
        retrieved_data = store.retrieve_data('EURUSD')
        pd.testing.assert_frame_equal(retrieved_data, expected_data)

    def test_normalize_data(self):
        # Create a sample dataframe
        data = {
            'feature1': [0, 5, 10],
            'feature2': [100, 200, 300]
        }
        df = pd.DataFrame(data, index=pd.date_range(start='2024-09-01', periods=3, freq='T'))
        store = DataStore(library_name='test', normalized=True)

        # Mock the scaler's fit_transform method
        with patch.object(store.scaler, 'fit_transform', return_value=[[0.0, 0.0], [0.5, 0.5], [1.0, 1.0]]):
            normalized_df = store.normalize_data(df)
            expected_df = pd.DataFrame({
                'feature1': [0.0, 0.5, 1.0],
                'feature2': [0.0, 0.5, 1.0]
            }, index=df.index)
            pd.testing.assert_frame_equal(normalized_df, expected_df)

if __name__ == '__main__':
    unittest.main()
