import logging
from plugins.operators.redshift_upsert_operator import RedshiftUpsertOperator
from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
 
log = logging.getLogger(__name__)


class AirflowCustomPlugin(AirflowPlugin):
    name = "custom_plugin"  # does not need to match the package name
    operators = [RedshiftUpsertOperator, S3ToRedshiftOperator]
    sensors = []
    hooks = [CrmHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []

class RedshiftUpsertOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 my_field,
                 *args,
                 **kwargs):
        super(MyOperator, self).__init__(*args, **kwargs)
        self.my_field = my_field

    def execute(self, context):
        hook = MyHook('my_conn')
        hook.my_method()

"""
 
Executes an upsert from one to another table in the same Redshift database by completely replacing the rows of target table with the rows in staging table that contain the same business key
 
:param src_redshift_conn_id: reference to a specific redshift database
:type src_redshift_conn_id: string
 
:param dest_redshift_conn_id: reference to a specific redshift database
:type dest_redshift_conn_id: string
 
:param src_table: reference to a specific table in redshift database
:type table: string
 
:param dest_table: reference to a specific table in redshift database
:type table: string
 
:param src_keys business keys that are supposed to be matched with dest_keys business keys in the target table
:type table: string
 
:param dest_keys business keys that are supposed to be matched with src_keys business keys in the source table
:type table: string
"""
 
  @apply_defaults
  def __init__(self, src_redshift_conn_id, dest_redshift_conn_id,
    src_table, dest_table, src_keys, dest_keys, *args, **kwargs):
  
    self.src_redshift_conn_id = src_redshift_conn_id
    self.dest_redshift_conn_id = dest_redshift_conn_id
    self.src_table = src_table
    self.dest_table = dest_table 
    self.src_keys = src_keys
    self.dest_keys = dest_keys
    super(RedshiftUpsertOperator , self).__init__(*args, **kwargs)
 
  def execute(self, context):
    self.hook = PostgresHook(postgres_conn_id=self.src_redshift_conn_id)
    conn = self.hook.get_conn()
    cursor = conn.cursor()
    log.info("Connected with " + self.src_redshift_conn_id)
    # build the SQL statement
    sql_statement = "begin transaction; "
    sql_statement += "delete from " + self.dest_table + " using " + self.src_table + " where "
    for i in range (0,len(self.src_keys)):
      sql_statement += self.src_table + "." + self.src_keys[i] + " = " + self.dest_table + "." + self.dest_keys[i]
      if(i < len(self.src_keys)-1):
        sql_statement += " and "
        
    sql_statement += "; "
    sql_statement += " insert into " + self.dest_table + " select * from " + self.src_table + " ; "
    sql_statement += " end transaction; "
  
    print(sql_statement)
    cursor.execute(sql_statement)
    cursor.close()
    conn.commit()
    log.info("Upsert command completed")


class S3ToRedshiftOperator(BaseOperator):
"""
Executes a LOAD command on a s3 CSV file into a Redshift table
:param redshift_conn_id: reference to a specific redshift database
:type redshift_conn_id: string
 
:param table: reference to a specific table in redshift database
:type table: string
 
:param s3_bucket: reference to a specific S3 bucket
:type s3_bucket: string
 
:param s3_access_key_id: reference to a specific S3 key
:type s3_key: string
 
:param s3_secret_access_key: reference to a specific S3 key
:type s3_key: string
 
:param delimiter: delimiter for CSV data
:type s3_key: string
 
:param region: location of the s3 bucket (eg. 'eu-central-1' or 'us-east-1')
:type s3_key: string
"""
 
  @apply_defaults
  def __init__(self, redshift_conn_id,table,s3_bucket,s3_path,s3_access_key_id,
    s3_secret_access_key,delimiter,region,*args, **kwargs):
  
    self.redshift_conn_id = redshift_conn_id
    self.table = table
    self.s3_bucket = s3_bucket
    self.s3_path = s3_path
    self.s3_access_key_id = s3_access_key_id
    self.s3_secret_access_key = s3_secret_access_key
    self.delimiter = delimiter 
    self.region = region
  
    super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
 
 
  def execute(self, context):
    self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    conn = self.hook.get_conn() 
    cursor = conn.cursor()
    log.info("Connected with " + self.redshift_conn_id)
  
    load_statement = """
      delete from {0};
      copy
      {0}
      from 's3://{1}/{2}'
      access_key_id '{3}' secret_access_key '{4}'
      delimiter '{5}' region '{6}' """.format(
    self.table, self.s3_bucket, self.s3_path,
    self.s3_access_key_id, self.s3_secret_access_key,
    self.delimiter, self.region)
  
    cursor.execute(load_statement)
    cursor.close()
    conn.commit()
    log.info("Load command completed")
 
  return True