#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.decorators import task

default_args = {
    'owner': 'himajas',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='create_session_summary1',
    default_args=default_args,
    schedule_interval=None,  
    start_date=datetime(2024, 10, 24),
    catchup=False,
) as dag:

    @task
    def create_session_summary():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run("""
            CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS
            SELECT DISTINCT 
                usc.USERID,
                usc.SESSIONID,
                st.TS,
                usc.CHANNEL
            FROM dev.raw_data.user_session_channel usc
            JOIN dev.raw_data.session_timestamp st
            ON usc.SESSIONID = st.SESSIONID
            QUALIFY ROW_NUMBER() OVER (PARTITION BY usc.SESSIONID ORDER BY st.TS) = 1;
        """)

    task_ss = create_session_summary()

