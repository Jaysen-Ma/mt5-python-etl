from ETL.features.base_feature import BaseFeature
import pandas as pd

class AverageCloseAllSymbols(BaseFeature):
    def __init__(self):
        super().__init__("Average_Close_All_Symbols")

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return df['close'].mean(axis=1)

class MedianVolumeAllSymbols(BaseFeature):
    def __init__(self):
        super().__init__("Median_Volume_All_Symbols")

    def compute(self, df: pd.DataFrame) -> pd.Series:
        return df['tick_volume'].median(axis=1)

# Similarly, define other universal metrics like correlation metrics, etc.
