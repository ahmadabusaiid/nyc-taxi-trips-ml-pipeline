from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import duckdb
import pandas as pd
import logging
import time

logger = logging.getLogger("airflow.task")
DB_LOC = '/opt/airflow/data/taxi_trips.db'
START_YEAR = datetime.now().year - 3

def append_to_table(c_df, y, m):
    while True:
        try:
            conn = duckdb.connect(DB_LOC)
            conn.register("temp_table", c_df)
            conn.execute("INSERT INTO trips SELECT * FROM temp_table")
            conn.close()
            logger.info(f"Data ingestion completed for {y}-{m:02d}")
            break
        except Exception as e:
            logger.warning(f"Failed to connect to DuckDB: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_table():
    conn = duckdb.connect(DB_LOC)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            passengers INTEGER,
            start_loc INTEGER,
            end_loc INTEGER,
            trip_distance FLOAT,
            total_amount FLOAT,
            tip_amount FLOAT
        )
    """)
    conn.close()

def get_min_year_missing():
    conn = duckdb.connect(DB_LOC)
    try:
        min_year = conn.execute("SELECT MAX(YEAR(start_time)) FROM trips").fetchone()[0]
        current_year = datetime.now().year
        if min_year is None or min_year < START_YEAR: 
            missing_year = START_YEAR
        elif min_year < current_year:
            missing_year = min_year + 1
        else:
            logger.info("All data is up-to-date.")
            missing_year = current_year + 1

        logger.info(f"Year selected for data ingestion: {missing_year}")
        return missing_year
    finally:
        conn.close()

def load_data(year, month, **kwargs):
    ti = kwargs['ti']
    missing_year = ti.xcom_pull(task_ids='get_min_year_missing')
    if missing_year <= year:
        yellow_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        green_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
        
        logger.info(f"Fetching data from {yellow_url} and {green_url}")
        
        try:
            yellow_df = pd.read_parquet(yellow_url)
            green_df = pd.read_parquet(green_url)
        except Exception as e:
            logger.warning(f"Could not load the Parquet file: {e}")
            return None

        if yellow_df is not None and green_df is not None:
            yellow_cleaned = clean_df(yellow_df)
            green_cleaned = clean_df(green_df)
            combined_df = pd.concat([yellow_cleaned, green_cleaned], ignore_index=True)
            append_to_table(combined_df, year, month)
    else:
        logger.info(f"Data for the year {year} already populated in table")

def clean_df(df):
    df.rename(
        columns={
            "tpep_pickup_datetime": "start_time",
            "tpep_dropoff_datetime": "end_time",
            "lpep_pickup_datetime": "start_time",
            "lpep_dropoff_datetime": "end_time",
            "passenger_count": "passengers",
            "PULocationID": "start_loc",
            "DOLocationID": "end_loc",
        },
        inplace=True
    )
    return df[[
        "start_time",
        "end_time",
        "passengers",
        "start_loc",
        "end_loc",
        "trip_distance",
        "total_amount",
        "tip_amount",
    ]]

def connect_db():
    conn = duckdb.connect(DB_LOC)
    logger.info("Connected to DuckDB database")
    conn.close()

with DAG(
    'taxi_trips_ingestion',
    start_date=datetime(START_YEAR, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Connect to the database
    connect_db_task = PythonOperator(
        task_id='connect_db',
        python_callable=connect_db
    )

    # Create the table if it doesn't exist
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    # Get the minimum year
    get_min_year_task = PythonOperator(
        task_id='get_min_year_missing',
        python_callable=get_min_year_missing,
    )

    for year in range(START_YEAR, datetime.now().year + 1):
        def create_load_tasks(year):

            def load_monthly_data(**kwargs):
                ti = kwargs['ti']
                for month in range(1, 13):
                    load_data(year, month, ti=ti)

            # Load monthly data for the specific year
            load_monthly_data_task = PythonOperator(
                task_id=f'append_data_{year}',
                python_callable=load_monthly_data,
                provide_context=True
            )

            connect_db_task >> create_table_task >> get_min_year_task >> load_monthly_data_task

        create_load_tasks(year)
