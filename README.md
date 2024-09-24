# ETL for MT5 Based Data

This repository contains an ETL (Extract, Transform, Load) pipeline designed to process and store financial data from MetaTrader5 into a S3 bucket using ArcticDB. The ETL process includes fetching raw data, applying various financial features, and storing both symbol-specific and universal features. 

## Folder Structure

### `src/ETL`
- **features**: Contains feature definitions categorized into symbol-specific and universal features.
  - **symbol_specific**: Features that are applied to individual financial symbols.
  - **universal**: Features that are computed across multiple symbols.
- **data_fetcher.py**: Handles fetching raw data from MetaTrader5.
- **data_store.py**: Manages storing and retrieving data from ArcticDB.
- **feature_engineer.py**: Applies various financial features to the data.
- **feature_definitions.py**: Defines the available features and their categories.

**main_etl.py**: The main ETL process that orchestrates data fetching, feature application, and data storage.

### `tests`
- Contains unit tests for various components of the ETL pipeline to ensure correctness and reliability.

## Key Components

### Data Fetching
The `DataFetcher` class is responsible for connecting to MetaTrader5 and fetching historical data for specified financial symbols.

### Feature Engineering
The `FeatureEngineer` class applies both symbol-specific and universal features to the fetched data. It uses configurations defined in `feature_config.json`.

### Data Storage
The `DataStore` class handles storing processed data into ArcticDB and retrieving it when needed.

### Main ETL Process
The `Mt5_ArcticDB_ETL` class orchestrates the entire ETL process, from fetching data to applying features and storing the results.

## Features

### Symbol-Specific Features
These features are applied to individual financial symbols and include various technical indicators like moving averages, momentum indicators, and volatility indicators.

### Universal Features
These features are computed across multiple symbols and include metrics like average close prices and median volumes.

## Testing
Unit tests are provided to ensure the correctness of the ETL components. Tests cover data fetching, feature application, and data storage.

## Getting Started

1. **Clone the repository**:
   ```sh
   git clone https://github.com/Jaysen-Ma/mt5-python-etl.git
   cd mt5-python-etl
   ```

2. **Install dependencies**:
   ```sh
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   Create a `.env` file with your MetaTrader5 login credentials:
   ```env
   mt5_broker_login=your_login
   mt5_broker_password=your_password
   mt5_broker_server=server
   ```

4. **Run the ETL process**:
   ```sh
   python src/main_etl.py
   ```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License.
