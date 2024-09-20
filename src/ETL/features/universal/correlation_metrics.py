import pandas as pd
from ETL.features.base_feature import BaseFeature

class ClosePriceCorrelation(BaseFeature):
    def __init__(self):
        """
        Initialize the Close Price Correlation feature.
        """
        super().__init__("Close_Price_Correlation")

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute the correlation of close prices across symbols.

        Args:
            df (pd.DataFrame): The input DataFrame containing 'close' prices for multiple symbols.

        Returns:
            pd.DataFrame: The computed correlation matrix.
        """
        return df.corr()