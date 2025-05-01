import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# task params
db_name = 'nbu_data.db'
currency_list = ['USD', 'EUR', 'GBP']
start_date_str = '20250216'
end_date_str = datetime.today().strftime('%Y%m%d')
url = 'https://bank.gov.ua/NBU_Exchange/exchange_site'

# setup logging
logging.basicConfig(
    level=logging.INFO,  # or DEBUG, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_currency_data(date: str = None, currency: str = None, ti=None) -> None:
    """
    Extract API currency exchange data and save it to db
    """
    headers = {
        'Accept': 'application/json'
    }

    # use default currency list if currency is not provided
    currency_to_process = [currency] if currency else currency_list

    # get API data for each currency code
    for currency in currency_to_process:
        params = {'date': date} if date else {'start': start_date_str, 'end': end_date_str}
        params.update({'valcode': currency.lower(),
                       'sort': 'exchangedate',
                       'order': 'desc',
                       'json': ''})

        response = requests.get(url, params=params, headers=headers)
        data = response.json()

        if not data:
            logging.error(f'Currency {currency} - no data found')
            return

        # save data to db
        date_val = date if date else f'{start_date_str} - {end_date_str}'
        logging.info(f'*** Saving data for currency code {currency} for {date_val}')

        # pass data to the next task instance
        ti.xcom_push(key='nbu_data', value=data)


def save_data(ti) -> None:
    """
    Save data to db
    """
    data = ti.xcom_pull(key='nbu_data', task_ids='fetch_nbu_data')
    if not data:
        raise ValueError("No data found")

    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')

    insert_query = """
    INSERT INTO currency_rate (currency_name, exchange_rate, currency_code, exchange_date, update_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT unique_rate_code_date 
    DO NOTHING;
    """
    for line in data:
        postgres_hook.run(insert_query, parameters=(line['enname'], line['rate'], line['cc'],
             datetime.strptime(line['exchangedate'], '%d.%m.%Y'), datetime.now()))


#------------------------------- DAG setup ----------------------------------------------------#

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'fetch_and_store_nbu_rates',
    default_args=default_args,
    description='DAG to fetch NBU rates and store results in Postgres',
    schedule_interval=timedelta(days=1),
)

fetch_nbu_data_task = PythonOperator(
    task_id='fetch_nbu_data',
    python_callable=get_currency_data,
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='postgres_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS currency_rate (
        id SERIAL PRIMARY KEY,
        currency_name TEXT,
        exchange_rate REAL,
        currency_code TEXT,
        exchange_date DATE,
        update_at TIMESTAMP,
        CONSTRAINT unique_rate_code_date UNIQUE (exchange_rate, currency_code, exchange_date)
    )
        """,
    dag=dag,
)

insert_nbu_data_task = PythonOperator(
    task_id='insert_nbu_data',
    python_callable=save_data,
    dag=dag,
)

# run tasks order
fetch_nbu_data_task >> create_table_task >> insert_nbu_data_task
