import pandas as pd
import pandas_ta as ta
import pytest
from ETL.features.symbol_specific.moving_averages import SMA, EMA

def test_SMA():
    data = {
        'close': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }
    df = pd.DataFrame(data)
    sma = SMA(length=3)
    result = sma.compute(df)
    expected = pd.Series([None, None, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], name="SMA_3")
    pd.testing.assert_series_equal(result, expected)

def test_EMA():
    data = {
        'close': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }
    df = pd.DataFrame(data)
    ema = EMA(length=3)
    result = ema.compute(df)
    # Calculate expected EMA manually or using pandas_ta
    expected = ta.ema(df['close'], length=3)
    pd.testing.assert_series_equal(result, expected)
