# Import necessary modules and libraries
# Documentation of TMDB: https://developer.themoviedb.org/reference/intro/getting-started

import requests
import requests.exceptions as requests_exceptions
import os
import json
import pathlib
import airflow.utils.dates
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
#from airflow.providers.postgres.operators.postgres import PostgresOperator

TMDB_API_TOKEN = "TMDB_API_TOKEN"
# Get the current date
current_date = datetime.now()
# Format the date as MM-DD
folder_name = current_date.strftime("%m-%d")

# Creating an Airflow DAG with specified settings
dag = DAG(
    dag_id="get_trending_movies",                  # Unique identifier for the DAG
    start_date=airflow.utils.dates.days_ago(1),  # Start date for the DAG (1 day ago)
    schedule_interval="@weekly",                # Schedule the DAG to run hourly
    max_active_runs=1,                         # Maximum concurrent runs of the DAG
)

def _get_trending_movies():

    API_KEY = Variable.get("TMDB_API_TOKEN")    
    url='https://api.themoviedb.org/3/trending/movie/day'
    
    try:
        
        response = requests.get(f"{url}?api_key={API_KEY}")
        #data = response.json()
        target_file = f"/tmp/data/{folder_name}/trending_movies"
        with open(target_file + '.json', "wb") as f:
            f.write(response.content)
        print(f" Movies written to {target_file} + '.json'")
    except requests_exceptions.MissingSchema:
        print(f"{url} appears to be an invalid URL.")
    except requests_exceptions.ConnectionError:
        print(f"Could not connect to {url}.")


get_movies = PythonOperator(
    task_id="get_movies", 
    python_callable=_get_trending_movies, 
    dag=dag
)

get_movies