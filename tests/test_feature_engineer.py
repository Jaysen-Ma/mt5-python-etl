import unittest
from ETL.feature_engineer import FeatureEngineer
from ETL.feature_definitions import function_space_symbol_specific, function_space_universal
import pandas as pd

class TestFeatureEngineer(unittest.TestCase):
    def setUp(self):
        self.feature_engineer = FeatureEngineer(function_space_symbol_specific, function_space_universal)

    def test_calculate_max_lookback(self):
        # Assuming the max length in feature definitions is 100
        self.assertEqual(self.feature_engineer.max_lookback, 100)

    def test_add_base_features(self):
        data = {
            'open': [1.1000, 1.1010, 1.1020],
            'high': [1.1010, 1.1020, 1.1030],
            'low': [1.0990, 1.1000, 1.1010],
            'close': [1.1005, 1.1015, 1.1025],
            'tick_volume': [1000, 1500, 2000]
        }
        df = pd.DataFrame(data, index=pd.date_range(start='2024-09-01', periods=3, freq='T'))
        df = self.feature_engineer.add_base_features(df)
        self.assertIn('returns', df.columns)
        self.assertIn('log_returns', df.columns)
        self.assertIn('minute', df.columns)
        self.assertIn('hour', df.columns)
        self.assertIn('day', df.columns)
        self.assertIn('day_of_week', df.columns)
        self.assertIn('minutes_in_bucket', df.columns)

    def test_apply_features_symbol_specific(self):
        data = {
            'open': [1.1000, 1.1010, 1.1020],
            'high': [1.1010, 1.1020, 1.1030],
            'low': [1.0990, 1.1000, 1.1010],
            'close': [1.1005, 1.1015, 1.1025],
            'tick_volume': [1000, 1500, 2000],
            'returns': [0.0, 0.00909090909090909, 0.009900990099009901],
            'log_returns': [0.0, 0.009048355144203516, 0.009756257808942875],
            'minute': [0, 1, 2],
            'hour': [0, 0, 0],
            'day': [1, 1, 1],
            'day_of_week': [5, 5, 5],
            'minutes_in_bucket': [0, 0, 0]
        }
        df = pd.DataFrame(data, index=pd.date_range(start='2024-09-01 00:00', periods=3, freq='T'))
        df = self.feature_engineer.apply_features(df, self.feature_engineer.feature_definitions_symbol)
        # Check if SMA_100 is present (assuming length=100 exists)
        self.assertIn('SMA_100', df.columns)
        # Check if SMA_100 has correct values
        self.assertAlmostEqual(df.loc['2024-09-01 00:02', 'SMA_100'], (1.1000 + 1.1010 + 1.1020) / 3, places=5)

if __name__ == '__main__':
    unittest.main()