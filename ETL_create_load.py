#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='create_load_tables_snowflake',
    default_args=default_args,
    schedule_interval=None,  
    start_date=datetime(2024, 10, 24),
    catchup=False,
) as dag:

    
    @task
    def create():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );

            CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );
        """)

    
    @task
    def load():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE OR REPLACE STAGE dev.raw_data.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');

            COPY INTO dev.raw_data.user_session_channel
            FROM @dev.raw_data.blob_stage/user_session_channel.csv;

            COPY INTO dev.raw_data.session_timestamp
            FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """)

    
    create_task = create()
    load_task = load()

    create_task >> load_task

