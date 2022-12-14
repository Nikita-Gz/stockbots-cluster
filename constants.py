# todo: make this a k8s secret?

def get_tracked_companies_list():
  return ['AAPL', 'META', 'MSFT', 'AMZN', 'GOOGL', 'AMD', 'NVDA', 'INTC']

def get_finnhub_api_key():
  return 'cd8nl0qad3i1cmedk0p0cd8nl0qad3i1cmedk0pg'

def get_raw_daily_prices_collection_name():
  return 'raw-daily-prices'

def get_preprocessed_daily_prices_collection_name():
  return 'preprocessed_daily_prices'
