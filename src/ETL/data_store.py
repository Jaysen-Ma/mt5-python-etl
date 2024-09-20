import arcticdb as adb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataStore:
    _arctic_instance = None  # Class-level variable to store the single Arctic instance
    def __init__(self, library_name: str) -> None:
        """
        Initialize the DataStore with a specified library name and normalization option.

        Args:
            library_name (str): The name of the library to store data in.
        """
        if DataStore._arctic_instance is None:
            DataStore._arctic_instance = adb.Arctic("lmdb://../TimeSeriesDB")
        self.library_name = library_name
        self.lib = DataStore._arctic_instance.get_library(self.library_name, create_if_missing=True)

    def store_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Store data for a given symbol in the ArcticDB library.

        Args:
            symbol (str): The financial instrument symbol.
            df (pd.DataFrame): The DataFrame containing the data to be stored.
        """
        self.lib.write(symbol, df)
        logger.info(f"Stored data for {symbol} in {self.library_name}")

    def retrieve_data(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve data for a given symbol from the ArcticDB library.

        Args:
            symbol (str): The financial instrument symbol.

        Returns:
            pd.DataFrame: The DataFrame containing the retrieved data.
        """
        try:
            return self.lib.read(symbol).data
        except KeyError:
            logger.error(f"No data found for symbol {symbol} in {self.lib.library_name}.")
            return pd.DataFrame()