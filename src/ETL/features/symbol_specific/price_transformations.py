import pandas as pd
import numpy as np
from ETL.features.base_feature import BaseFeature

class LogReturns(BaseFeature):
    def __init__(self):
        """
        Initialize the Log Returns feature.
        """
        super().__init__("Log_Returns")

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Log Returns for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed Log Returns values.
        """
        return np.log(df['close'] / df['close'].shift(1))

class PctChange(BaseFeature):
    def __init__(self, periods: int):
        """
        Initialize the Percentage Change feature.

        Args:
            periods (int): The number of periods over which to compute the percentage change.
        """
        super().__init__(f"Pct_Change_{periods}")
        self.periods = periods

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Percentage Change for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed Percentage Change values.
        """
        return df['close'].pct_change(periods=self.periods)

class ZScore(BaseFeature):
    def __init__(self, window: int):
        """
        Initialize the Z-Score feature.

        Args:
            window (int): The rolling window size for the Z-Score computation.
        """
        super().__init__(f"Z_Score_{window}")
        self.window = window

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Z-Score for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices.

        Returns:
            pd.Series: The computed Z-Score values.
        """
        rolling_mean = df['close'].rolling(window=self.window).mean()
        rolling_std = df['close'].rolling(window=self.window).std()
        return (df['close'] - rolling_mean) / rolling_std