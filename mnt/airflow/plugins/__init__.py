from airflow.plugins_manager import AirflowPlugin
from plugins.operators.redshift_upsert_operator import RedshiftUpsertOperator
from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator

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