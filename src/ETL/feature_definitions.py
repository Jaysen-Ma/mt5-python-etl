from ETL.features.symbol_specific.moving_averages import SMA, EMA, WMA
from ETL.features.symbol_specific.momentum_indicators import RSI, MACD, STOCH
from ETL.features.symbol_specific.volatility_indicators import BBANDS, ATR, Volatility
from ETL.features.symbol_specific.volume_indicators import OBV, CMF
from ETL.features.symbol_specific.price_transformations import LogReturns, PctChange, ZScore

from ETL.features.universal.global_metrics import AverageCloseAllSymbols, MedianVolumeAllSymbols
from ETL.features.universal.correlation_metrics import ClosePriceCorrelation

# Symbol-specific feature classes
symbol_specific_features = {
    "Moving_Averages": [SMA, EMA, WMA],
    "Momentum_Indicators": [RSI, MACD, STOCH],
    "Volatility_Indicators": [BBANDS, ATR, Volatility],
    "Volume_Indicators": [OBV, CMF],
    "Price_Transformations": [LogReturns, PctChange, ZScore],
}

# Universal feature classes
universal_features = {
    "Global_Metrics": [AverageCloseAllSymbols, MedianVolumeAllSymbols],
    "Correlation_Metrics": [ClosePriceCorrelation],
}
