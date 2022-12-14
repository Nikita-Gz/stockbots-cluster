import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'start_date': datetime(2020, 11, 18),
     'max_active_runs': 1,
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}

with airflow.DAG('dag_test_spark',
                  default_args=default_args) as dag:
    spark_operator = SparkSubmitOperator(
        task_id='spaek1',
        conn_id='spark_local_conn',
        application="/opt/airflow/dags/example_spark.py",
        total_executor_cores=1,
        application_args=["{{ ds }}"],
        executor_memory="2g",
        conf={
            "spark.driver.maxResultSize": "2g"
        }
    )
