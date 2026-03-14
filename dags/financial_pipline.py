from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def extract_and_load_raw_data():
    """Extract S&P 500 (AAPL) data for 1 month and load to Postgres."""
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="1mo")
    df.reset_index(inplace=True)
    df['Date'] = df['Date'].dt.date
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    engine = create_engine(POSTGRES_CONN)
    df.to_sql('raw_financial_data', engine, if_exists='replace', index=False)

with DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='End-to-end ETL for Financial Data Mart',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'etl', 'datamart'],
) as dag:

    extract_load_raw = PythonOperator(
        task_id='extract_and_load_raw',
        python_callable=extract_and_load_raw_data,
    )

    transform_to_datamart = PostgresOperator(
        task_id='transform_to_datamart',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS financial_data_mart;
            CREATE TABLE financial_data_mart AS
            SELECT 
                "Date",
                "Close" AS closing_price,
                "Volume" AS trading_volume,
                ("High" - "Low") AS daily_volatility
            FROM raw_financial_data
            WHERE "Volume" > 0;
        """,
    )

    data_quality_check = PostgresOperator(
        task_id='data_quality_check',
        postgres_conn_id='postgres_default',
        sql="""
            SELECT 1/ (
                CASE 
                    WHEN COUNT(*) = 0 THEN 0
                    WHEN MIN(closing_price) < 0 THEN 0
                    ELSE 1
                END
            ) FROM financial_data_mart;
        """,
    )

    extract_load_raw >> transform_to_datamart >> data_quality_check