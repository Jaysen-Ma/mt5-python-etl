## A local folder to store the transformed data.
### symbol_specific 
Each key within the 'symbol_specific' library is a symbol (e.g. EURUSD), the value is a feature engineered dataframe of the key (require only 1 OHLCV)

The columns of the dataframe is of the form ['open', ..., 'log_returns', ..., 'CMF_20', ..., 'Pct_Change_10', ...]

### universeal
The only key is 'Universal_Features', and the value is the dataframe with features tied to more than 1 symbol. 

A column could be the ratio of the symbol with lowest RSI over the averaged RSI over a group of symbols at that moment of time (require more than 1 OHLCV)