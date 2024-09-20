import datetime
import pandas as pd
import MetaTrader5 as mt5
import logging
from retry import retry

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        """
        Initialize the DataFetcher by setting up the MetaTrader5 connection.
        
        Raises:
            RuntimeError: If MetaTrader5 initialization fails.
        """
        if not mt5.initialize():
            logger.error("Failed to initialize MetaTrader5")
            raise RuntimeError("MetaTrader5 initialization failed")
        else:
            logger.info("MetaTrader5 initialized successfully")
    
    @retry(tries=2, delay=2, backoff=2)
    def fetch_data(self, symbol: str, start_time: datetime.datetime, end_time: datetime.datetime) -> pd.DataFrame:
        """
        Fetch base historical data (open, high, low, close, tick_volume, spread) for a given symbol between start_time and end_time.
        
        Args:
            symbol (str): The financial instrument symbol to fetch data for.
            start_time (datetime.datetime): The start time for the data range.
            end_time (datetime.datetime): The end time for the data range.
        
        Returns:
            pd.DataFrame: A DataFrame containing the historical data with time as the index.
                          Returns an empty DataFrame if no data is fetched.
        """
        logger.info(f"Fetching data for {symbol} from {start_time} to {end_time}")
        
        # Ensure MetaTrader5 is initialized before fetching data
        if not mt5.initialize():
            logger.error("MetaTrader5 re-initialization failed")
            return pd.DataFrame()
        
        rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, start_time, end_time)
        if rates is None:
            logger.warning(f"No data returned for {symbol} from MetaTrader5.")
            return pd.DataFrame()
        df = pd.DataFrame(rates).drop('real_volume', axis=1, errors='ignore')  # Vantage's 'real_volume' is populated with 0 
        if df.empty:
            return df
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('time', inplace=True)
        return df

    def get_symbol_info(self, symbol):
        
        """
        Retrieve information about a symbol from MetaTrader5.

        Args:
            symbol (str): The financial instrument symbol to retrieve information for.

        Returns:
            dict: A dictionary containing the symbol information, currently includes 'name', 'description', and 'last_timestamp'.
                  Returns an empty dictionary if the symbol is not found.
        """

        info = mt5.symbol_info(symbol)
        if info is None:
            logger.error(f"Symbol {symbol} not found in MetaTrader5.")
            raise ValueError(f"Symbol {symbol} not found.")
        info_dict = info._asdict()
        return {
            "name": info_dict.get('name'),
            "description": info_dict.get('description'),
            'last_timestamp': None,
            # Add other necessary information as needed, info_dict contains a lot other information
        }