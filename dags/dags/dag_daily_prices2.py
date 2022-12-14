from airflow.decorators import dag, task
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import airflow
import datetime
import pendulum
import os
import time
import logging


from connect_to_mongo import connect_to_mongo_db
from constants import get_tracked_companies_list, get_finnhub_api_key
#from spark_connection import setup_connection

default_task_args = {
  'retries': 999999,
  'retry_delay': datetime.timedelta(minutes=1)
}
