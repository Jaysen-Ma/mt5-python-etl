import pandas as pd
import pandas_ta as ta
from ETL.features.base_feature import BaseFeature

class BBANDS(BaseFeature):
    def __init__(self, length: int, std: int):
        """
        Initialize the Bollinger Bands (BBANDS) feature.

        Args:
            length (int): The period length for the BBANDS.
            std (int): The number of standard deviations for the BBANDS.
        """
        super().__init__(f"BBANDS_{length}_{std}")
        self.length = length
        self.std = std

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the Bollinger Bands (BBANDS) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.DataFrame: The computed BBANDS values.
        """
        bbands = ta.bbands(df['close'], length=self.length, std=self.std)
        return bbands

class ATR(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Average True Range (ATR) feature.

        Args:
            length (int): The period length for the ATR.
        """
        super().__init__(f"ATR_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Average True Range (ATR) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'high', 'low', and 'close' prices.

        Returns:
            pd.Series: The computed ATR values.
        """
        return ta.atr(df['high'], df['low'], df['close'], length=self.length)

class Volatility(BaseFeature):
    def __init__(self, window: int):
        """
        Initialize the Volatility feature.

        Args:
            window (int): The rolling window size for the volatility computation.
        """
        super().__init__(f"Volatility_{window}")
        self.window = window

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Volatility for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'returns'.

        Returns:
            pd.Series: The computed Volatility values.
        """
        return df['returns'].rolling(window=self.window).std()