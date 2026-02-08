from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

start_date = '2021-01-01'
end_date = '2022-01-01'

db_connection = 'gp_my_schema'
db_scheme = 'my_schema'

db_load_fact_function = 'f_load_delta_partitions'
db_fact_arg_dict = {
    'sales':'ext_sales',
    'plan':'ext_plan',
}
db_fact_masterdata_query = f"select {db_scheme}.{db_load_fact_function}(%(src_tab_name)s, %(tgt_tab_name)s, 'date', '{start_date}', '{end_date}');"

db_load_dim_function = 'f_load_full'
db_dim_arg_dict = {
    'price':'ext_price',
    'product':'ext_product',
    'chanel':'ext_chanel',
    'region':'ext_region',
}
db_dim_masterdata_query = f"select {db_scheme}.{db_load_dim_function}(%(src_tab_name)s, %(tgt_tab_name)s, '1=1', TRUE);"

db_load_mart_function = 'f_load_mart'
db_mart_arg_dict = {
    '202101':'YYYYMM',
}
db_mart_masterdata_query = f"select {db_scheme}.{db_load_mart_function}(%(date_format)s, %(month)s);"

default_args = {
    'depends_on_past': False,
    'owner': 'my_schema',
    'start_date': datetime(2026, 1, 26),
    'retries': 0,
    'retry_detay': timedelta(minutes=5),
    'email': ['my_address@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'my_schema_main',
    max_active_runs = 3,
    schedule_interval = None,
    default_args = default_args,
    catchup = False,
    description = 'DAG by my_schema student',
) as dag:

    task_start = DummyOperator(task_id="start")

    with TaskGroup("load_fact_tables") as load_fact_tables:
        for key, value in db_fact_arg_dict.items():
            task = PostgresOperator(
                task_id=f"load_fact_table_{key}",
                postgres_conn_id=db_connection,
                sql=db_fact_masterdata_query,
                parameters={
                    'src_tab_name':f'{db_scheme}.{value}',
                    'tgt_tab_name':f'{db_scheme}.{key}',
                }
            )

    with TaskGroup("load_dim_tables") as load_dim_tables:
        for key, value in db_dim_arg_dict.items():
            task = PostgresOperator(
                task_id=f"load_dim_table_{key}",
                postgres_conn_id=db_connection,
                sql=db_dim_masterdata_query,
                parameters={
                    'src_tab_name':f'{db_scheme}.{value}',
                    'tgt_tab_name':f'{db_scheme}.{key}',
                }
            )

    with TaskGroup("load_mart_table") as load_mart_table:
        for key, value in db_mart_arg_dict.items():
            task = PostgresOperator(
                task_id=f"load_mart_table_{key}",
                postgres_conn_id=db_connection,
                sql=db_mart_masterdata_query,
                parameters={
                    'date_format':f'{value}',
                    'month':f'{key}',
                }
            )

    task_end = DummyOperator(task_id="end")

    task_start >> load_fact_tables >> load_dim_tables >> load_mart_table >> task_end
