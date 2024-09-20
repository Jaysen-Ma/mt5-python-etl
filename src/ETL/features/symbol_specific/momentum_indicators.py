import pandas as pd
import pandas_ta as ta
from ETL.features.base_feature import BaseFeature

class RSI(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Relative Strength Index (RSI) feature.

        Args:
            length (int): The period length for the RSI.
        """
        super().__init__(f"RSI_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Relative Strength Index (RSI) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed RSI values.
        """
        return ta.rsi(df['close'], length=self.length)

class MACD(BaseFeature):
    def __init__(self, fast: int, slow: int, signal: int):
        """
        Initialize the Moving Average Convergence Divergence (MACD) feature.

        Args:
            fast (int): The fast period length for the MACD.
            slow (int): The slow period length for the MACD.
            signal (int): The signal period length for the MACD.
        """
        super().__init__(f"MACD_{fast}_{slow}_{signal}")
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the Moving Average Convergence Divergence (MACD) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.DataFrame: The computed MACD values.
        """
        macd = ta.macd(df['close'], fast=self.fast, slow=self.slow, signal=self.signal)
        return macd

class STOCH(BaseFeature):
    def __init__(self, k: int, d: int):
        """
        Initialize the Stochastic Oscillator (STOCH) feature.

        Args:
            k (int): The %K period length for the STOCH.
            d (int): The %D period length for the STOCH.
        """
        super().__init__(f"STOCH_{k}_{d}")
        self.k = k
        self.d = d

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the Stochastic Oscillator (STOCH) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'high', 'low', and 'close' prices.

        Returns:
            pd.DataFrame: The computed STOCH values.
        """
        stoch = ta.stoch(df['high'], df['low'], df['close'], k=self.k, d=self.d)
        return stoch