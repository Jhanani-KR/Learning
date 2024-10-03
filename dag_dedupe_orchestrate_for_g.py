
from airflow import DAG
from airflow.models import Variable
import json

from dedupe_orchestration.dedupe_history_collate_for_g import dedupeOperator
from datetime import datetime, timedelta

serialized_email_list = Variable.get('mailing_list')

email_list = json.loads(serialized_email_list)

email_list_str = ', '.join(email_list)


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


dag = DAG(
    dag_id='job_create_dedupe_tables_for_g',
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=1),
    description='DAG to process and migrate DEDUPED data from Es to Postgresql',
    # schedule_interval=timedelta(minutes=90),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath="./",
)

latestrecord_task = dedupeOperator(
    task_id='latestrecord_task',
    es_conn_id='DWH_ElasticSearch', 
    base_metadata_conn_id = 'DWH_Datahub_connection',
    base_metadata_table_name = Variable.get('dedupe_metadata_table', default_var='dedupe_metadata_table'),
    base_metadata_schema = 'hubdwh',
    dest_schema_name = 'dwh_deduped',
    dest_schema_conn_id = 'DWH_Datahub_connection',
    dag=dag
)

latestrecord_task  #>> inter_task >> final_task
