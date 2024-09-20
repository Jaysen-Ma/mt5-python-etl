from abc import ABC, abstractmethod
import pandas as pd

class BaseFeature(ABC):
    """
    Abstract base class for all features.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute the feature and return as a pandas Series.
        """
        pass
