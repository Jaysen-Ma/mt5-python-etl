import arcticdb as adb
import pandas as pd
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DataStore:
    _arctic_instance = None  # Class-level variable to store the single Arctic instance
    def __init__(self, library_name: str) -> None:
        """
        Initialize the DataStore with a specified library name and normalization option.

        Args:
            library_name (str): The name of the library to store data in.
        """
        if DataStore._arctic_instance is None:
            DataStore._arctic_instance = self._initialize_arcticdb()
        self.library_name = library_name
        self.lib = DataStore._arctic_instance.get_library(self.library_name, create_if_missing=True)

    def _initialize_arcticdb(self) -> adb.Arctic:
        """
        Initialize the ArcticDB connection to AWS S3 using credentials from environment variables.

        Returns:
            adb.Arctic: An instance of ArcticDB connected to AWS S3.
        """
        load_dotenv()
        # Retrieve configurations from environment variables
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION')
        s3_bucket = os.getenv('S3_BUCKET_NAME')
        s3_endpoint = os.getenv('S3_ENDPOINT')
        use_ssl = os.getenv('USE_SSL', 'true').lower() == 'true'
        # path_prefix = os.getenv('PATH_PREFIX', '')

        if not all([aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket, s3_endpoint]):
            logger.error("Missing AWS S3 configuration in environment variables.")
            raise ValueError("Incomplete AWS S3 configuration.")

        # Construct the connection string based on SSL usage
        protocol = 's3s' if use_ssl else 's3'
        connection_string = f"{protocol}://{s3_endpoint}:{s3_bucket}"
        
        # Append query parameters
        query_params = f"?region={aws_region}&access={aws_access_key_id}&secret={aws_secret_access_key}"
        # if path_prefix:
        #     query_params += f"&path_prefix={path_prefix}"
        
        connection_string += f"?{query_params}"

        try:
            arctic = adb.Arctic(connection_string)
            logger.info(f"Connected to ArcticDB at {connection_string}")
            return arctic
        except Exception as e:
            logger.error(f"Failed to connect to ArcticDB at {connection_string} : {e}")
            raise e
        
    def store_data(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Store data for a given symbol in the ArcticDB library.

        Args:
            symbol (str): The financial instrument symbol.
            df (pd.DataFrame): The DataFrame containing the data to be stored.
        """
        try:
            self.lib.write(symbol, df)
            logger.info(f"Stored data for symbol: {symbol} in library: {self.library_name}")
        except Exception as e:
            logger.error(f"Failed to store data for symbol {symbol} in library {self.library_name}: {e}")
            raise e

    def retrieve_data(self, symbol: str) -> pd.DataFrame:
        """
        Retrieve data for a given symbol from the ArcticDB library.

        Args:
            symbol (str): The financial instrument symbol.

        Returns:
            pd.DataFrame: The DataFrame containing the retrieved data.
        """
        try:
            data = self.lib.read(symbol).data
            logger.info(f"Retrieved data for symbol: {symbol} from library: {self.library_name}")
            return data
        except KeyError:
            logger.error(f"No data found for symbol {symbol} in library {self.library_name}.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to retrieve data for symbol {symbol} from library {self.library_name}: {e}")
            return pd.DataFrame()