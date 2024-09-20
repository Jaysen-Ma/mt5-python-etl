import pandas as pd
import pandas_ta as ta
from ETL.features.base_feature import BaseFeature

class OBV(BaseFeature):
    def __init__(self):
        """
        Initialize the On-Balance Volume (OBV) feature.
        """
        super().__init__("OBV")

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the On-Balance Volume (OBV) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices and 'volume'.

        Returns:
            pd.Series: The computed OBV values.
        """
        return ta.obv(df['close'], df['tick_volume'])

class CMF(BaseFeature):
    def __init__(self, length: int):
        """
        Initialize the Chaikin Money Flow (CMF) feature.

        Args:
            length (int): The period length for the CMF.
        """
        super().__init__(f"CMF_{length}")
        self.length = length

    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the Chaikin Money Flow (CMF) for the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'high', 'low', 'close', and 'volume' prices.

        Returns:
            pd.Series: The computed CMF values.
        """
        return ta.cmf(df['high'], df['low'], df['close'], df['tick_volume'], length=self.length)