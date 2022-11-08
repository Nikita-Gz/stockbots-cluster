import finnhub
import websocket
import json
import datetime
import pymongo
import os

from companylist import get_tracked_companies_list

# todo: make this a k8s secret?
finnhub_key = 'cd8nl0qad3i1cmedk0p0cd8nl0qad3i1cmedk0pg'
tracked_companies = get_tracked_companies_list()

mongo_client = pymongo.MongoClient("mongodb://mongodb/", username='root', password=os.getenv('MONGODB_ROOT_PASSWORD'))
db = mongo_client["tradebots-db"]
realtime_prices_collection = db['realtime-prices-finnhub']

# turns {(company, time): {'price':x 'volume':y}} into just records list
def unwind_temp_storage(storage):
  all_records = []
  for (company, seconds_timestamp), price_data in storage.items():
    full_record = {
      'company': company,
      'seconds_timestamp': seconds_timestamp,
      'price': price_data['price'],
      'volume': price_data['volume']
    }
    all_records.append(full_record)
  print(f'Got {len(all_records)} records in temp storage')
  return all_records


def save_temp_storage_to_mongodb(storage):
  records_list = unwind_temp_storage(storage)
  if len(records_list) == 0:
    print('Skipping saving no records')
    return

  print(f'Saving {len(records_list)} temp records to db')
  response = realtime_prices_collection.insert_many(records_list)
  print(f'Inserted {len(response.inserted_ids)} rows out of {len(records_list)}')


class WebhookDataHolder:
  def __init__(self):
    self.temporary_storage = dict()
    self.seconds_to_keep_temp_storage = 5
    self.last_save_time = None

webhook_data = WebhookDataHolder()


def on_message(ws, message):
  global webhook_data

  message_json = json.loads(message)
  if message_json['type'] == 'trade':
    trades = message_json['data']
    for trade in trades:
      company = trade['s']
      price = trade['p']
      milliseconds_timestamp = trade['t']
      seconds_timestamp = milliseconds_timestamp // 1000
      volume = trade['v']

      key = (company, seconds_timestamp)
      existing_volume = webhook_data.temporary_storage.get(key, dict()).get('volume', 0)

      record_to_fill = {'price': price, 'volume': existing_volume + volume}
      webhook_data.temporary_storage[(company, seconds_timestamp)] = record_to_fill
  else:
    print(f'Got message: {message_json}')

  # save everything in temp storage if time elapsed is more than the treshold
  seconds_elapsed_since_last_save = (datetime.datetime.now() - webhook_data.last_save_time).seconds
  if seconds_elapsed_since_last_save >= webhook_data.seconds_to_keep_temp_storage:
    save_temp_storage_to_mongodb(webhook_data.temporary_storage)
    webhook_data.temporary_storage.clear()
    webhook_data.last_save_time = datetime.datetime.now()


def on_error(ws, error):
  print(error)


def on_close(ws, a, b):
  print(a, b)
  print("### closed ###")


def on_open(ws):
  global webhook_data
  webhook_data.last_save_time = datetime.datetime.now()
  for company_to_track in tracked_companies:
    print(f'Subscribing to {company_to_track}')
    ws.send('{"type":"subscribe","symbol":"' + company_to_track + '"}')
  print('Done. Listening')


websocket.enableTrace(False)
ws = websocket.WebSocketApp("wss://ws.finnhub.io?token={}".format(finnhub_key),
                          on_message = on_message,
                          on_error = on_error,
                          on_close = on_close)
ws.on_open = on_open
ws.run_forever()
