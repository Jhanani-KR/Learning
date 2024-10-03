import pandas as pd
import time
import psycopg2
import psycopg2.extras
import logging
from airflow.utils.decorators import apply_defaults

from airflow.models.baseoperator import BaseOperator
from elasticsearch import Elasticsearch, exceptions
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class dedupeOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                es_conn_id,
                base_metadata_conn_id,
                base_metadata_table_name = None,
                base_metadata_schema = None,
                dest_schema_name = None,
                dest_schema_conn_id = None,
                *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.es_conn_id = es_conn_id
        self.es_params = BaseHook.get_connection(es_conn_id)
        self.es_url = f"http://{self.es_params.host}:{self.es_params.port}"
        self.es_username = self.es_params.login
        self.es_password = self.es_params.password
        self.metadata_table = base_metadata_table_name
        self.metadata_conn_id = base_metadata_conn_id
        self.metadata_schema = base_metadata_schema
        self.dest_schema_name = dest_schema_name
        self.dest_schema_conn_id = dest_schema_conn_id

        self.es = Elasticsearch(
            [{'host': self.es_params.host, 'port': self.es_params.port, 'scheme': 'http'}],
            basic_auth=(self.es_username, self.es_password),
            request_timeout=120,
            max_retries=10,
            retry_on_timeout=True
        )

        pg_conn = BaseHook.get_connection(self.dest_schema_conn_id)
        self.pg_conn_details = {
            'host': pg_conn.host,
            'port': pg_conn.port,
            'dbname': pg_conn.schema,
            'user': pg_conn.login,
            'password': pg_conn.password
        }

        # self.pg_conn = psycopg2.connect(**self.pg_conn_details)
        # self.pg_cur = self.pg_conn.cursor()
        
    def get_pg_connection(self):
        return psycopg2.connect(**self.pg_conn_details)

    def _fetch_meta_data(self, cursor, sql):
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        if data:
            self.logger.info(f'Fetched information from metadata table. There are {len(data)} tables active')
        else:
            self.logger.info('No active tables present in metadata table')
        return data

    def _metadata_data(self):
        try:
            metadata_hook = PostgresHook(postgres_conn_id = self.metadata_conn_id)
            metadata_conn = metadata_hook.get_conn()
            metadata_cursor = metadata_conn.cursor()
            sql = f"SELECT * FROM {self.metadata_schema}.{self.metadata_table} WHERE (checkpoint_date IS NOT NULL) AND is_active = 1 AND group_id = 1 ORDER BY config_id"
            metadata_result = self._fetch_meta_data(metadata_cursor, sql)
            metadata_cursor.close()
            return metadata_result
        except Exception as e:
            self.logger.error(f"An error occurred on metadata table query: {str(e)}")
            raise
        finally:
            try:
                if metadata_conn:
                    metadata_conn.close()
            except NameError:
                pass

    def _metadata_success_update(self, table_name, updated_checkpoint_date ):
        try:
            metadata_hook = PostgresHook(postgres_conn_id = self.metadata_conn_id)
            metadata_conn = metadata_hook.get_conn()
            metadata_cursor = metadata_conn.cursor()
            update_stmt = f"UPDATE {self.metadata_schema}.{self.metadata_table} SET checkpoint_date = '{updated_checkpoint_date}' WHERE table_name='{table_name}'"
            metadata_cursor.execute(update_stmt)

            metadata_conn.commit()
            metadata_cursor.close()
            self.logger.info(f"Updated {self.metadata_table} and updated {table_name}")
        except Exception as e:
            self.logger.error(f"An error occurred on _metadata_success_update : {str(e)}")
            raise
        finally:
            if metadata_conn:
                metadata_conn.close()

    def fetch_es_data_scroll(self, index, metadata, scroll_time, page_size=10000):
        unique_keys =  metadata['unique_keys'] 
        src_tbl_fields = metadata['query_fields']
        src_fields_list = src_tbl_fields.split(',')
       
        query_body = {
                "size": page_size,
                "_source": src_fields_list,
                "aggs": {
                    "unique_customers": {
                        "terms": {
                            "field": unique_keys,
                            "size": page_size
                        },
                        "aggs": {
                            "latest_record": {
                                "top_hits": {
                                    "sort": [
                                        {"_doc": {"order": "desc"}}
                                    ],
                                    "size": 1
                                }
                            }
                        }
                    }
                }
            }

        page = self.es.search(
            index=index,
            scroll=scroll_time,
            body= query_body
        )
    
        scroll_id  = page['_scroll_id']
        hits = page['hits']['hits']
        yield hits
    
        while len(hits) > 0:
            try:
                page = self.es.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = page['_scroll_id']
                hits = page['hits']['hits']
                if hits:
                    yield hits
            except exceptions.ConnectionTimeout as e:
                print(f"Scroll timeout: {e}")
                time.sleep(5)
                continue
        
        print(f"scrollid :  {scroll_id}")
        # if scroll_id > 0:    
        #     self.es.clear_scroll(scroll_id=scroll_id)
    
    def convert_to_postgresql_type(self, value, pg_type):
        if value is None or pd.isna(value) or value == 'NA':
            return None

        if pg_type in ['integer', 'bigint', 'smallint']:
            try:
                return int(value)
            except (ValueError, OverflowError):
                return None
        elif pg_type in ['numeric', 'real', 'double precision']:
            try:
                return float(value)
            except ValueError:
                return None
        elif pg_type in ['boolean']:
            try:
                return bool(value)
            except ValueError:
                return None
        elif pg_type in ['text', 'varchar', 'char']:
            return str(value)
        elif pg_type in ['timestamp', 'date']:
            try:
                return pd.to_datetime(value)
            except (ValueError, TypeError):
                return None
        else:
            return value
        

    def get_column_data_types(self, table_name):
        query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        """

        print(f"============== select schema query {query}")
        with self.get_pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = cursor.fetchall()
                # Return as a dictionary {column_name: data_type}
                return {col[0]: col[1] for col in columns}


    # Method to insert data into PostgreSQL
    def insert_into_postgres(self, column_data_types, table_name, data):
        #column_data_types = self.get_column_data_types(table_name)

        # Prepare a list of columns based on the existing table schema
        columns = list(column_data_types.keys())

        insert_query = f"""
        INSERT INTO {self.dest_schema_name}.{table_name} ({', '.join(columns)}) 
        VALUES ({', '.join(['%s'] * len(columns))})
        """
        print(f"============== insert schema query {insert_query}")

        with self.get_pg_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data)
            conn.commit()


    def process_and_insert_data(self, table_name, metadata):
        all_rows = []
        dest_table_name = metadata['dest_table_name']
        dest_coldata_type = self.get_column_data_types(dest_table_name)
        for es_data_batch in self.fetch_es_data_scroll(table_name, metadata,'10m'):
            for doc in es_data_batch:
                source = doc['_source']
                row = tuple(self.convert_to_postgresql_type(source.get(col), dest_coldata_type.get(col)) for col in dest_coldata_type)
                all_rows.append(row)

            self.insert_into_postgres(dest_coldata_type, dest_table_name, all_rows)
            all_rows.clear()



    def execute(self, context):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        try:
            metadata_information = self._metadata_data()

            if metadata_information:
                for metadata in metadata_information:
                    table_name = f"{metadata['source_table_name']}"
                    # revised_checkpoint_date = self._validate_checkpointdate(metadata)
                    self.logger.info(f"Data start processing for Index : {table_name}")

                    self.process_and_insert_data(table_name, metadata)

        except Exception as e:
            self.logger.error(f"An error occurred on Executor : {str(e)}")
        finally:
            self.logger.info("Process completed")
