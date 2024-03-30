# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os



dag_path = os.getcwd()


def transform_data():
   
    taxi= pd.read_csv(f"{dag_path}/raw/Taxi_Trips.csv",low_memory=False)
    def convert_col_names(df):
    ## converting column names to a lower case and adding an underscore
         return df.rename(columns=lambda x: x.lower().replace(" ","_"))
    taxi["trip_start_timestamp"]= pd.to_datetime(taxi["trip_start_timestamp"])
    taxi["trip_end_timestamp"]= pd.to_datetime(taxi["trip_end_timestamp"])

    taxi = convert_col_names(taxi)

    taxi.to_csv(f"{dag_path}/processed/processed_moniepoint.csv",index=False)

  



def load_data():
        conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
        c = conn.cursor()
        c.execute('''
                    CREATE TABLE "taxi_trips2" (
                        
                        "taxi_id" TEXT(200),
                        "trip_start_timestamp" datetime,
                        "trip_end_timestamp" datetime,
                        "trip_seconds" REAL,
                        "trip_miles" REAL,
                        "pickup_census_tract" REAL,
                        "dropoff_census_tract" REAL,
                        "pickup_community_area" REAL,
                        "dropoff_community_area" REAL,
                        "fare" REAL,
                        "tips" REAL,
                        "tolls" REAL,
                        "extras" REAL,
                        "trip_total" REAL,
                        "payment_type" TEXT(200),
                        "company" TEXT(200),
                        "pickup_centroid_latitude" REAL,
                        "pickup_centroid_longitude" REAL,
                        "pickup_centroid_location" TEXT(200),
                        "dropoff_centroid_latitude" REAL,
                        "dropoff_centroid_longitude" REAL,
                        "dropoff_centroid__location" TEXT(200)
                                                );
                                        ''')
        records = pd.read_csv(f"{dag_path}/processed/processed_moniepoint.csv")
        records.to_sql('taxi_trips2', conn, if_exists='replace', index=False)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'moniepoint',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)  


task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag
)


task_1 >> task_2
    