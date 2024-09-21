import datetime
import pandas as pd
import numpy as np
import logging
from retry import retry
from concurrent.futures import ProcessPoolExecutor, as_completed
from scipy import stats
from typing import List, Dict, Optional, Tuple, Any
from dotenv import load_dotenv
import MetaTrader5 as mt5
from os import environ
import json

from ETL.data_fetcher import DataFetcher
from ETL.feature_engineer import FeatureEngineer
from ETL.data_store import DataStore
from ETL.feature_definitions import symbol_specific_features, universal_features

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(ch)

class Mt5_ArcticDB_ETL:
    """
    ETL class for processing and storing financial data from MetaTrader5 to a local folder with ArcticDB.
    """

    def __init__(self, 
                 metadata_path: str = 'TimeSeriesDB/metadata.json') -> None:
        """
        Initialize the ETL process with the given library name, metadata path, and database path.
        """
        load_dotenv()
        
        # MetaTrader 5 login
        authorized = mt5.login(
            login=environ.get("mt5_broker_login"),
            password=environ.get("mt5_broker_password"),
            server=environ.get("mt5_broker_server")
        )
        
        self.metadata_path: str = metadata_path # path to the json metadata file
        self.fetcher: DataFetcher = DataFetcher() # for fetching raw data
        
        # Initialize FeatureEngineer with class-based features
        self.feature_engineer: FeatureEngineer = FeatureEngineer(
            symbol_features=symbol_specific_features,
            universal_features=universal_features
        )

        # Separate stores for symbol-specific and universal features
        self.store_symbol_specific = DataStore(library_name='symbol_specific')
        self.store_universal = DataStore(library_name='universal')

        self.symbols: List[str] = []
        self.last_processed: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = self.load_metadata()
        self.universal_symbol: str = 'Universal_Features'
        self.data_start_time = datetime.datetime(2024, 9, 1, 0, 0, 0)

    def load_metadata(self) -> Dict[str, Any]:
        """
        Load metadata from the specified path. If the file does not exist or is corrupted,
        create a new metadata structure.
        """
        try:
            with open(self.metadata_path, 'r') as f:
                metadata = json.load(f)
                logger.info("Metadata loaded successfully.")
                return metadata
        except:
            logger.warning("Metadata file not found. Creating a new one.")
            metadata = {
                "symbols": {},
                "etl_runs": []
            }
            self.metadata_path = 'TimeSeriesDB/metadata.json'
            self.save_metadata(metadata)
            return metadata

    def save_metadata(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Save the current metadata to the specified path.
        """
        if metadata is None:
            metadata = self.metadata
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
            logger.info("Metadata saved successfully.")

    def add_symbols(self, symbols: List[str]) -> None:
        """
        Add new symbols to the ETL process and update the metadata accordingly.
        """
        self.symbols.extend(symbols)
        for symbol in symbols:
            if symbol not in self.metadata['symbols']:
                symbol_info = self.fetcher.get_symbol_info(symbol)
                self.metadata['symbols'][symbol] = symbol_info
        self.save_metadata()
        logger.info(f"Added symbols: {symbols}")
    
    def get_last_timestamp(self, symbol: str) -> Optional[str]:
        """
        Retrieve the last processed timestamp for a given symbol from the metadata.
        """
        return self.metadata['symbols'][symbol].get('last_timestamp')
    
    @retry(tries=3, delay=2, backoff=2)
    def process_symbol(self, symbol: str, end_time: datetime.datetime) -> Tuple[str, Optional[pd.DataFrame]] :
        """
        Process a given symbol by fetching data, adding features, and storing the processed data.

        Args:
            symbol (str): The financial instrument symbol to process.
            end_time (datetime.datetime): The end time for the data range to process.

        Returns:
            Tuple[str, Optional[pd.DataFrame]]: A tuple containing the symbol and the processed DataFrame.
                                                Returns (symbol, None) if no new data is processed.
        """
        try:
            logger.info(f"Starting processing for symbol: {symbol}")
            last_timestamp = self.get_last_timestamp(symbol)
            if last_timestamp:
                logger.info(f"Last timestamp for {symbol}: {last_timestamp}")
                # Determine the required lookback
                lookback_minutes = self.feature_engineer.max_lookback
                logger.info(f"Lookback period: {lookback_minutes} minutes")
                # Convert last_timestamp string to datetime
                last_timestamp_dt = datetime.datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')
                # Calculate new start_time by subtracting lookback
                start_time = last_timestamp_dt - datetime.timedelta(minutes=lookback_minutes)
                # Ensure start_time does not go before the earliest possible date
                earliest_date = self.data_start_time
                if start_time < earliest_date:
                    start_time = earliest_date
                logger.info(f"Calculated start time: {start_time}")
            else:
                # No previous data, start from default start_time
                start_time = self.data_start_time
                logger.info(f"No previous data found. Using default start time: {start_time}")

            # Fetch data
            logger.info(f"Fetching data for {symbol} from {start_time} to {end_time}")
            data = self.fetcher.fetch_data(symbol, start_time, end_time)
            if data.empty:
                logger.info(f"No new data for {symbol}")
                return symbol, None

            # Check data quality
            logger.info(f"Checking data quality for {symbol}")
            data = self.check_data_quality(data)

            # Add base features
            logger.info(f"Adding base features for {symbol}")
            data = self.feature_engineer.add_base_features(data)

            # Apply symbol-specific features
            logger.info(f"Applying symbol-specific features for {symbol}")
            symbol_feature_classes = []
            for category, features in symbol_specific_features.items():
                symbol_feature_classes.extend(features)
            data = self.feature_engineer.apply_symbol_features(data, symbol_feature_classes)

            # Add 'symbol' and 'date_id' columns
            # logger.info(f"Adding 'symbol' and 'date_id' columns for {symbol}")
            # data['symbol'] = symbol
            # data['date_id'] = pd.to_datetime(data.index.date)

            # Since we fetched additional data for lookback, determine the incremental data to store
            if last_timestamp:
                # Filter data to only include new data after last_timestamp
                new_data = data[data.index > last_timestamp_dt]
                logger.info(f"Filtered new data for {symbol} after last timestamp")
            else:
                new_data = data

            if new_data.empty:
                logger.info(f"No new data to store for {symbol} after filtering with lookback")
                return symbol, None

            # Store symbol specific data
            logger.info(f"Storing data for {symbol}")
            self.store_symbol_specific.store_data(symbol, new_data)

            # Update metadata with the latest timestamp from new_data
            self.metadata['symbols'][symbol]['last_timestamp'] = new_data.index.max().strftime('%Y-%m-%d %H:%M:%S')
            self.save_metadata()

            logger.info(f"Processed and stored data for {symbol}")
            return symbol, new_data
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return symbol, None

    def check_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check the quality of the data by identifying missing values, duplicate timestamps, and extreme values.
        """
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.sum() > 0:
            logger.warning(f"Missing values detected: {missing_values}")
            # Optionally, handle missing values here (e.g., fill or drop)

        # Check for duplicate timestamps
        duplicates = df.index.duplicated()
        if duplicates.sum() > 0:
            logger.warning(f"Duplicate timestamps detected: {duplicates.sum()}")
            df = df[~duplicates]

        # Check for extreme values (Z-score > 3)
        for column in ['open', 'high', 'low', 'close']:
            if column in df.columns:
                z_scores = np.abs(stats.zscore(df[column].dropna()))
                extreme_values = (z_scores > 3).sum()
                if extreme_values > 0:
                    logger.warning(f"Extreme values detected in {column}: {extreme_values}")
                    # Optionally, handle extreme values here (e.g., cap or remove)

        return df

    def run_etl(self) -> None:
        """
        Run the entire ETL process: fetch data, process symbols, and compute universal features.
        """
        logger.info("Starting ETL process")
        end_time = datetime.datetime.now()
        symbol_data: Dict[str, pd.DataFrame] = {}
        cleaned_data_start_times: List[datetime.datetime] = []

        # Use ProcessPoolExecutor for multiprocessing
        with ProcessPoolExecutor(max_workers=4) as executor:
            # Submit all symbol processing tasks
            future_to_symbol = {executor.submit(self.process_symbol, symbol, end_time): symbol for symbol in self.symbols}

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol, data = future.result()
                    if data is not None:
                        symbol_data[symbol] = data
                        cleaned_data_start_times.append(data.index.min())
                except Exception as e:
                    logger.error(f"Exception occurred while processing {symbol}: {e}")

        # Under development
        # Compute and store universal features
        # if symbol_data:
        #     # Combine data from all symbols to compute universal features
        #     logger.info("Combining data from all symbols for universal features")
        #     combined_df = pd.concat(symbol_data.values(), axis=1, join='inner', keys=symbol_data.keys())
        #     # Flatten MultiIndex columns
        #     combined_df.columns = ['_'.join(col) for col in combined_df.columns]
        #     # Apply universal features
        #     logger.info("Applying universal features")
        #     universal_feature_classes = []
        #     for category, features in universal_features.items():
        #         universal_feature_classes.extend(features)
        #     combined_df = self.feature_engineer.apply_universal_features(combined_df, universal_feature_classes)

            # Handle any additional processing
            # combined_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            # combined_df.fillna(method='ffill', inplace=True)
            # combined_df.fillna(method='bfill', inplace=True)
            # combined_df.fillna(0, inplace=True)  # Final fallback

            # Store universal features
            # logger.info("Storing universal features")
            # self.store_universal.store_data(self.universal_symbol, combined_df)

            # logger.info("Stored universal features")

        # Log ETL run details
        etl_run = {
            "timestamp": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "processed_symbols": list(symbol_data.keys()),
            "status": "Completed"
        }
        self.metadata['etl_runs'].append(etl_run)
        self.save_metadata()

        logger.info("ETL process completed")

# Example usage

if __name__ == "__main__":
    # Initialize ETL class
    etl = Mt5_ArcticDB_ETL(metadata_path = 'TimeSeriesDB/metadata.json')
    
    # Add symbols, for example from a text file
    with open('src/data_selection/core_symbols.txt', 'r') as f:
        symbols = [line.strip() for line in f.readlines()]
    etl.add_symbols(symbols) 
    
    # Run ETL process
    etl.run_etl()
