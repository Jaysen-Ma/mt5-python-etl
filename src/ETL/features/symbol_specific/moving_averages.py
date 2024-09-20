import pandas as pd
import pandas_ta as ta
from ETL.features.base_feature import BaseFeature

class SMA(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Simple Moving Average (SMA) feature.

        Args:
            length (int): The period length for the SMA.
        """
        super().__init__(f"SMA_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Simple Moving Average (SMA) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed SMA values.
        """
        return ta.sma(df['close'], length=self.length)

class EMA(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Exponential Moving Average (EMA) feature.

        Args:
            length (int): The period length for the EMA.
        """
        super().__init__(f"EMA_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Exponential Moving Average (EMA) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed EMA values.
        """
        return ta.ema(df['close'], length=self.length)

class WMA(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Weighted Moving Average (WMA) feature.

        Args:
            length (int): The period length for the WMA.
        """
        super().__init__(f"WMA_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Weighted Moving Average (WMA) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed WMA values.
        """
        return ta.wma(df['close'], length=self.length)

class HMA(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Hull Moving Average (HMA) feature.

        Args:
            length (int): The period length for the HMA.
        """
        super().__init__(f"HMA_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Hull Moving Average (HMA) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed HMA values.
        """
        return ta.hma(df['close'], length=self.length)

class VWAP(BaseFeature):
    def __init__(self):
        """
        Initialize the Volume Weighted Average Price (VWAP) feature.
        """
        super().__init__("VWAP")

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Volume Weighted Average Price (VWAP) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'high', 'low', 'close', and 'volume' prices.

        Returns:
            pd.Series: The computed VWAP values.
        """
        return ta.vwap(df['high'], df['low'], df['close'], df['volume'])
