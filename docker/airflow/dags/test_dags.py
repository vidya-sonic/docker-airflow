import datetime as dt

from airflow import DAG
from airflow.operators.redshift_upsert_plugin import RedshiftUpsertOperator
from airflow.operators.redshift_load_plugin import S3ToRedshiftOperator

default_args = {
  'owner': 'me',
  'start_date': dt.datetime(2020, 5, 1),
  'retries': 2,
  'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG('redshift-demo',
  default_args=default_args,
  schedule_interval='@once'
)

upsert = RedshiftUpsertOperator(
  task_id='upsert',
  src_redshift_conn_id="pc_redshift",
  dest_redshift_conn_id="pc_redshift",
  src_table="stage_customer",
  dest_table="customer",
  src_keys=["id"],
  dest_keys=["id"],
  dag = dag
)
 
load = S3ToRedshiftOperator(
  task_id="load",
  redshift_conn_id="pc_redshift",
  table="stage_customer",
  s3_bucket="vid-airflow-source-data",
  s3_path="customer.csv",
  s3_access_key_id="key",
  s3_secret_access_key="key",
  delimiter=",",
  region="us-east-1",
  dag=dag
)
 
load >> upsert