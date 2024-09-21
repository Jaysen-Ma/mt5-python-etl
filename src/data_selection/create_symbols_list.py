forex_pairs = [
    "EURUSD", "GBPUSD", "USDJPY", "NZDUSD", "USDCAD", "AUDUSD", "USDCHF",
    "USDCNH", "USDHKD", "USDMXN", "USDNOK", "USDPLN", "USDSEK", "USDSGD",
    "USDTRY", "USDZAR", "USDINR", "USDBRL", "USDCLP", "USDCOP", "USDIDR",
    "USDKRW", "USDTWD", "USDTHB", "USDHUF", "USDCZK", "USDDKK"
]

indices = [
    "DJ30", "SP500", "NAS100", "US2000", "VIX", "EU50", "FRA40", "UK100",
    "GER40", "ES35", "Nikkei225", "CHINA50", "HK50", "JPN225ft", "HKTECH",
    "DJ30ft", "NAS100ft", "SP500ft", "CHINA50ft", "FRA40ft", "GER40ft",
    "UK100ft", "HK50ft"
]

commodities = [
    "XAGUSD", "XAUUSD", "XPDUSD", "XPTUSD", "COPPER-C", "CL-OIL", "GAS-C",
    "NG-C", "GASOIL-C", "USOUSD", "UKOUSD", "UKOUSDft", "Soybean-C", "Wheat-C",
    "Cocoa-C", "Coffee-C", "Cotton-C", "OJ-C", "Sugar-C"
]

cryptocurrencies = [
    "BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD", "BCHUSD", "EOSUSD", "XLMUSD",
    "BTCBCH", "BTCETH", "ADAUSD", "DOGUSD", "DOTUSD", "LNKUSD", "SOLUSD", 
    "UNIUSD", "ALGUSD", "AVAUSD", "BATUSD", "FILUSD", "IOTUSD", "GRTUSD",
    "MKRUSD", "NEOUSD", "SHBUSD", "TRXUSD", "ZECUSD", "ATMUSD", "AXSUSD",
    "BNBUSD", "CRVUSD", "ETCUSD", "INCUSD", "LRCUSD", "NERUSD", "ONEUSD",
    "SANUSD", "SUSUSD", "XTZUSD", "GRTUSD"
]

stocks = [
    "AAPL", "AMAZON", "BOEING", "CISCO", "EXXON", "GOOG", "IBM", "INTEL",
    "MSFT", "NVIDIA", "ORCL", "PFIZER", "PG", "TSLA", "META", "SNOW", "COIN",
    "ALIBABA", "BAIDU", "TOYOTA", "TSM", "VISA", "SHELL"
]

bonds_and_interest_rates = [
    "LongGilt", "USNote10Y", "EURIBOR3M", "EUB10Y", "EUB5Y", "EUB2Y", "EUB30Y"
]

miscellaneous = [
    "USDX", "LOGC", "SGP20", "FI", "COR"
]

# Concatenate all symbols into a single list
all_symbols = (
    forex_pairs + indices + commodities + cryptocurrencies +
    stocks + bonds_and_interest_rates + miscellaneous
)

# Write the symbols to a text file
with open('core_symbols.txt', 'w') as f:
    for symbol in all_symbols:
        f.write(symbol + '\n')