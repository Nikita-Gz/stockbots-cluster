from airflow.models.connection import Connection
from airflow import settings

def setup_connection():
  c = Connection(
    conn_id="spark_local_conn",
    conn_type="spark",
    description="Local Spark Connection",
    host="local")

  session = settings.Session()
  session.add(c)
  session.commit()
