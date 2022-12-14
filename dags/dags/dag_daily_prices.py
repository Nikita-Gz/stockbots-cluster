from airflow.decorators import dag, task
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import airflow
import datetime
import pendulum
import os
import time
import logging

import finnhub

from connect_to_mongo import connect_to_mongo_db
from constants import get_tracked_companies_list, get_finnhub_api_key, get_raw_daily_prices_collection_name
from spark_connection import setup_connection

default_task_args = {
  'retries': 100,
  'retry_delay': datetime.timedelta(minutes=1)
}


@dag(
  dag_id="daily-prices-fetcher",
  schedule_interval="@daily",
  start_date=pendulum.datetime(2021, 12, 12, tz="UTC"),
  catchup=True,
  dagrun_timeout=datetime.timedelta(hours=12),
  max_active_runs=1,
  default_args = default_task_args
)
def DailyPricesDAG():
  # tasks:
  # 0. setup spark connection
  # 1. get daily prices from finnhub and save them to a raw data collection
  # 2. preprocess daily prices and save them to a different collection
  # 3. clean up raw data collection

  def str_date_to_timestamp(date: str):
    return int(pendulum.parse(date).timestamp())

  def save_records_to_db(records: list):
    if len(records) == 0:
      logging.info('No records to save to db')
      return

    db = connect_to_mongo_db()
    db[get_raw_daily_prices_collection_name()].insert_many(records)
  
  def get_price_from_company_on_date(company: str, date: str):
    finnhub_client = finnhub.Client(api_key=get_finnhub_api_key())
    timestamp_to_fetch = str_date_to_timestamp(date)
    logging.info(f'Fetching price for {company} on {date} (timestamp: {timestamp_to_fetch})')
    response = finnhub_client.stock_candles(company, 'D', timestamp_to_fetch, timestamp_to_fetch)

    # validating response status. Warn if "no_data". Warn
    status = response.get('s', 'No status in response')
    if status == 'no_data':
      logging.warning(f'No data for {company} on {date}')
      return None
    elif status != 'ok':
      raise Exception(f'Error getting price for {company} on {date}: {status}. Response: {response}')

    # checks if all of the keys are present
    expected_keys = ['c', 'h', 'l', 'o', 's', 't', 'v']
    if not all(key in response for key in expected_keys):
      raise Exception(f'Error getting price for {company} on {date}: missing keys in response. Response: {response}')

    # unwinds resposne dict of lists into dict of values, except for 't' and 's' keys
    response = {key: value[0] for key, value in response.items() if key not in ['t', 's']}
    response['company'] = company
    response['timestamp'] = timestamp_to_fetch

    return response

  """
  @task(task_id="setup_spark_connection")
  def setup_spark_connection():
    try:
      setup_connection()
    except Exception as e:
      logging.warning("Spark connection already exists. Skipping setup.")
  """

  # clears existing daily prices with the same date
  @task(task_id="clear_existing_daily_prices")
  def clear_existing_daily_prices(ds=None, **kwargs):
    db = connect_to_mongo_db()
    db[get_raw_daily_prices_collection_name()].delete_many({'timestamp': str_date_to_timestamp(ds), 'company': {'$in': get_tracked_companies_list()}})

  @task(task_id="get_daily_prices")
  def get_daily_prices(ds=None, **kwargs):
    price_records = []
    for company in get_tracked_companies_list():
      price_record = get_price_from_company_on_date(company, ds)
      if price_record is not None:
        price_records.append(price_record)
      time.sleep(1)
    print(f'Saving {len(price_records)} records to db')
    save_records_to_db(price_records)

  preprocess_prices = SparkSubmitOperator(
    task_id='preprocessor_spark_operator',
    conn_id='spark_local_conn',
    application="/opt/airflow/dags/process_daily_prices.py",
    total_executor_cores=1,
    application_args=["{{ ds }}"],
    executor_memory="2g",
    conf={ "spark.driver.maxResultSize": "2g" })
  
  
  @task(task_id="clean_up_raw_data")
  def clean_up_raw_data(ds=None, **kwargs):
    db = connect_to_mongo_db()
    db[get_raw_daily_prices_collection_name()].delete_many({'timestamp': str_date_to_timestamp(ds)})

  clear_existing_daily_prices() >> get_daily_prices() >> preprocess_prices

dag = DailyPricesDAG()
