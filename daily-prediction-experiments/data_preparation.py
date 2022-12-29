# this file:
# 1) loads preprocessed data from mongodb
# 2) converts it to a pandas dataframe
# 3) keeps only specific columns
# 4) to each row, appends data from N previous days for the same company
# 5) from each row, removes non-target columns for the latest day
# 6) scales the data (StandardScaler)
# 7) returns the dataframe

import pandas as pd
import pymongo
import os
import re
import numpy as np
from sklearn.preprocessing import StandardScaler

from constants import get_preprocessed_daily_prices_collection_name


def connect_to_mongo_db(db_name='tradebots-db'):
  mongo_client = pymongo.MongoClient("mongodb://mongodb/", username='root', password=os.getenv('MONGODB_ROOT_PASSWORD'))
  db = mongo_client[db_name]
  return db


def get_preprocessed_daily_prices_dataframe():
  columns_to_keep = ['timestamp', 'company', 'c_diff', 'o_diff', 'l_diff', 'h_diff', 'v_diff']
  db = connect_to_mongo_db()
  preprocessed_prices_collection = db[get_preprocessed_daily_prices_collection_name()]
  preprocessed_prices_dicts = [document for document in preprocessed_prices_collection.find({}, {"_id": 0})]
  preprocessed_prices_dataframe = pd.DataFrame(preprocessed_prices_dicts)
  preprocessed_prices_dataframe = preprocessed_prices_dataframe[columns_to_keep]
  preprocessed_prices_dataframe = preprocessed_prices_dataframe.sort_values(by=['company', 'timestamp'], ascending=True)
  return preprocessed_prices_dataframe


def append_previous_days_data(dataframe, days_to_append=5):
  dataframe = dataframe.copy()
  for i in range(1, days_to_append + 1):
    dataframe[f'c_diff_{i}'] = dataframe.groupby('company')['c_diff'].shift(i)
    dataframe[f'o_diff_{i}'] = dataframe.groupby('company')['o_diff'].shift(i)
    dataframe[f'l_diff_{i}'] = dataframe.groupby('company')['l_diff'].shift(i)
    dataframe[f'h_diff_{i}'] = dataframe.groupby('company')['h_diff'].shift(i)
    dataframe[f'v_diff_{i}'] = dataframe.groupby('company')['v_diff'].shift(i)
  
  # drop rows with NaN values
  dataframe = dataframe.dropna()
  
  # print first 50 rows just to check
  print(dataframe.head(50))

  return dataframe


def remove_non_target_columns_from_last_day(dataframe, target_column='c_diff'):
  dataframe = dataframe.copy()

  # note: last day columns don't have a number at the end
  last_day_columns_to_remove = [column for column in dataframe.columns if not re.search(r'\d+$', column)]
  last_day_columns_to_remove.remove(target_column)

  dataframe = dataframe.drop(columns=last_day_columns_to_remove)
  return dataframe


def scale_data(dataframe):
  dataframe = dataframe.copy()
  scaler = StandardScaler()
  scaler.fit(dataframe)
  scaled_data = scaler.transform(dataframe)
  scaled_dataframe = pd.DataFrame(scaled_data, columns=dataframe.columns)
  return scaled_dataframe


def prepare_data_for_prediction(days_to_append=5):
  dataframe = get_preprocessed_daily_prices_dataframe()
  dataframe = append_previous_days_data(dataframe, days_to_append=days_to_append)
  dataframe = remove_non_target_columns_from_last_day(dataframe)
  dataframe = scale_data(dataframe)
  return dataframe

