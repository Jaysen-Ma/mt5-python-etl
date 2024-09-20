import importlib
from typing import List, Type
from features.base_feature import BaseFeature
import logging

logger = logging.getLogger(__name__)

def load_feature_classes(feature_names: List[str], category: str, feature_type: str) -> List[Type[BaseFeature]]:
    """
    Dynamically load feature classes based on feature names.

    Args:
        feature_names (List[str]): List of feature class names to load.
        category (str): Category of the features (e.g., 'symbol_specific', 'universal').
        feature_type (str): Specific feature category (e.g., 'Moving_Averages').

    Returns:
        List[Type[BaseFeature]]: List of feature classes.
    """
    feature_classes = []
    for feature_name in feature_names:
        module_path = f"features.{category}.{feature_type.lower()}.{feature_name.lower()}"
        class_name = feature_name
        try:
            module = importlib.import_module(module_path)
            feature_cls = getattr(module, class_name)
            feature_classes.append(feature_cls)
        except (ModuleNotFoundError, AttributeError) as e:
            logger.error(f"Error loading feature {feature_name}: {e}")
    return feature_classes
