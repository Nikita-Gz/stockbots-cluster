import sys
from random import random
from operator import add

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

import pendulum
import pymongo
import json
import os
import sys

from connect_to_mongo import connect_to_mongo_db
from constants import get_raw_daily_prices_collection_name, get_preprocessed_daily_prices_collection_name

db = connect_to_mongo_db()
raw_prices_collection = db[get_raw_daily_prices_collection_name()]
processed_prices_collection = db[get_preprocessed_daily_prices_collection_name()]

def str_date_to_timestamp(date: str):
  return int(pendulum.parse(date).timestamp())

# somehow get this parameter from airflow
args = sys.argv[1:]
print(args)
date_to_process = str_date_to_timestamp(args[0])

# fields in raw records:
# timestamp
# company
# c
# o
# l
# h
# v

# fields in processed records:
# timestamp
# date
# company
# c
# o
# l
# h
# v
# c diff
# o diff
# l diff
# h diff
# v diff

def endSparkSession(spark):
  spark.stop()

spark = SparkSession\
.builder\
.appName("DailyPricesProcessor")\
.getOrCreate()

raw_prices_dicts = [document for document in raw_prices_collection.find({}, {"_id": 0, "timestamp": 1, "company": 1, "c": 1, "o": 1, "l": 1, "h": 1, "v": 1})]

# just in case, converts c, o, l, h, v to float
for raw_prices_dict in raw_prices_dicts:
  for field in ['c', 'o', 'l', 'h', 'v']:
    raw_prices_dict[field] = float(raw_prices_dict[field])

print(f'Raw records length: {len(raw_prices_dicts)}')
if len(raw_prices_dicts) == 0 :
  print("No raw prices found")
  endSparkSession(spark)
  sys.exit(0)


raw_df = spark.createDataFrame(Row(**row) for row in raw_prices_dicts)

# algorithm:
# 1) convert timestamp to dates
# 2) process separately by company
# 3) compute diff values

# converting timestamps to dates
with_dates = raw_df.withColumn("date", f.to_date(f.col("timestamp").cast(TimestampType())))

window = Window.partitionBy("company").orderBy("date")
with_diffs = with_dates.\
  withColumn("c_diff", f.lit(1.0) - f.col("c") / (f.lag(f.col("c"), 1, 0).over(window) + f.lit(sys.float_info.min))).\
  withColumn("o_diff", f.lit(1.0) - f.col("o") / (f.lag(f.col("o"), 1, 0).over(window) + f.lit(sys.float_info.min))).\
  withColumn("l_diff", f.lit(1.0) - f.col("l") / (f.lag(f.col("l"), 1, 0).over(window) + f.lit(sys.float_info.min))).\
  withColumn("h_diff", f.lit(1.0) - f.col("h") / (f.lag(f.col("h"), 1, 0).over(window) + f.lit(sys.float_info.min))).\
  withColumn("v_diff", f.lit(1.0) - f.col("v") / (f.lag(f.col("v"), 1, 0).over(window) + f.lit(sys.float_info.min)))

# remove existing records with same company and date, as they will be replaced
distinct_companies_and_dates = with_diffs.select('company', 'timestamp').distinct()
records_to_delete = distinct_companies_and_dates.toPandas()
companies_to_delete = list(records_to_delete['company'].values)
timestamps_to_delete = [int(timestamp) for timestamp in list(records_to_delete['timestamp'].values)]
processed_prices_collection.delete_many({'company': {'$in': companies_to_delete},
                                         'timestamp': {'$in': timestamps_to_delete}})

records = with_diffs.toPandas().to_dict('records')

# converts datetime.date in list of dicts to string
records = [{**record, 'date': str(record['date'])} for record in records]
processed_prices_collection.insert_many(records)

endSparkSession(spark)
