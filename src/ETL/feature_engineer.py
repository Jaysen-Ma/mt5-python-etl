import pandas as pd
import numpy as np
import logging
import json
from itertools import product
from typing import List, Type, Optional
from ETL.features.base_feature import BaseFeature

logger = logging.getLogger(__name__)

class FeatureEngineer:
    def __init__(self, symbol_features: dict, universal_features: dict) -> None:
        """
        Initialize the FeatureEngineer with symbol-specific and universal feature definitions.

        Args:
            symbol_features (dict): Dictionary of symbol-specific feature categories and their classes.
            universal_features (dict): Dictionary of universal feature categories and their classes.
        """
        self.symbol_features = symbol_features
        self.universal_features = universal_features
        self.max_lookback = self.calculate_max_lookback()
        logger.info(f"Calculated maximum lookback: {self.max_lookback} minutes")

        # Load feature configuration from JSON
        with open('ETL/feature_config.json', 'r') as f:
            self.feature_config = json.load(f)

    def calculate_max_lookback(self) -> int:
        """
        Calculate the maximum lookback required based on feature definitions.
        Assumes that features with a 'lookback' attribute indicate a lookback.

        Returns:
            int: The maximum lookback period in minutes.
        """
        max_lookback = 0
        for category, features in self.symbol_features.items():
            for feature_cls in features:
                if hasattr(feature_cls, 'lookback'):
                    max_lookback = max(max_lookback, feature_cls.lookback)
        for category, features in self.universal_features.items():
            for feature_cls in features:
                if hasattr(feature_cls, 'lookback'):
                    max_lookback = max(max_lookback, feature_cls.lookback)
        return max_lookback
    
    def add_base_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add base features that are required by other features to the DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame containing financial data.

        Returns:
            pd.DataFrame: DataFrame with added base features.
        """
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        df['minute'] = df.index.minute
        df['hour'] = df.index.hour
        df['day'] = df.index.day
        df['day_of_week'] = df.index.dayofweek

        # Define 'minutes_in_bucket' based on 'minute'
        thresholds = [15, 30, 45]
        groups = [0, 1, 2, 3]  # Adjust group labels as needed
        df['minutes_in_bucket'] = np.select(
            [df['minute'] < thresholds[0],
             df['minute'] < thresholds[1],
             df['minute'] < thresholds[2]],
            groups[:3],
            default=groups[3]
        )
        return df

    def get_feature_info(self, feature_name: str) -> Optional[dict]:
        """
        Get the feature information from the JSON configuration.

        Args:
            feature_name (str): The name of the feature.

        Returns:
            dict: The feature information dictionary.
        """
        for category, features in self.feature_config['symbol_specific'].items():
            if feature_name in features:
                return features[feature_name]
        return None

    def generate_param_combinations(self, parameters: dict) -> List[dict]:
        """
        Generate all combinations of parameters.

        Args:
            parameters (dict): The parameters dictionary.

        Returns:
            List[dict]: A list of parameter combinations.
        """
        # Filter out non-parameter keys like 'description'
        param_keys = {k: v for k, v in parameters.items() if k != 'description'}
        if not param_keys:
            return [{}]  # Return a list with an empty dict if there are no parameters
        keys, values = zip(*param_keys.items())
        return [dict(zip(keys, v)) for v in product(*values)]

    def apply_symbol_features(self, df: pd.DataFrame, feature_classes: List[Type[BaseFeature]]) -> pd.DataFrame:
        """
        Apply symbol-specific features to the DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame to which features will be applied.
            feature_classes (List[Type[BaseFeature]]): List of feature classes to apply.

        Returns:
            pd.DataFrame: DataFrame with applied symbol-specific features.
        """
        for feature_cls in feature_classes:
            try:
                feature_name = feature_cls.__name__
                feature_info = self.get_feature_info(feature_name)
                if feature_info:
                    param_combinations = self.generate_param_combinations(feature_info)
                    for param_combination in param_combinations:
                        feature_instance = feature_cls(**param_combination)
                        result = feature_instance.compute(df)
                        if isinstance(result, pd.DataFrame):
                            for col in result.columns:
                                df[f"{feature_instance.name}_{col}"] = result[col]
                        else:
                            df[feature_instance.name] = result
                        logger.debug(f"Applied feature: {feature_instance.name}")
                else:
                    feature_instance = feature_cls()  # Default instantiation
                    result = feature_instance.compute(df)
                    if isinstance(result, pd.DataFrame):
                        for col in result.columns:
                            df[f"{feature_instance.name}_{col}"] = result[col]
                    else:
                        df[feature_instance.name] = result
                    logger.debug(f"Applied feature: {feature_instance.name}")
            except TypeError as te:
                logger.error(f"TypeError applying feature {feature_cls.__name__}: {te}")
            except Exception as e:
                logger.error(f"Error applying feature {feature_cls.__name__}: {e}")
        return df

    def apply_universal_features(self, df: pd.DataFrame, feature_classes: List[Type[BaseFeature]]) -> pd.DataFrame:
        """
        Apply universal features to the DataFrame.

        Args:
            df (pd.DataFrame): Combined DataFrame across all symbols.
            feature_classes (List[Type[BaseFeature]]): List of universal feature classes to apply.

        Returns:
            pd.DataFrame: DataFrame with applied universal features.
        """
        for feature_cls in feature_classes:
            try:
                feature_instance = feature_cls()
                result = feature_instance.compute(df)
                if isinstance(result, pd.DataFrame):
                    for col in result.columns:
                        df[f"{feature_instance.name}_{col}"] = result[col]
                else:
                    df[feature_instance.name] = result
                logger.debug(f"Applied universal feature: {feature_instance.name}")
            except TypeError as te:
                logger.error(f"TypeError applying universal feature {feature_cls.__name__}: {te}")
            except Exception as e:
                logger.error(f"Error applying universal feature {feature_cls.__name__}: {e}")
        return df