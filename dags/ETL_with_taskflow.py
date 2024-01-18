# imports important for Airflow
import pendulum
from airflow.decorators import dag, task
import logging
# Import Modules for code
import json
import requests
import os

from sqlalchemy.orm import Session, declarative_base
from sqlalchemy import create_engine, Index, select, Integer, Column, String, PrimaryKeyConstraint
from sqlalchemy.exc import IntegrityError
import pandas as pd
from pandas import DataFrame, json_normalize
import datetime as dt
import psycopg2
from transformer import transform_weatherAPI

POSTGRES_KEY = os.getenv("POSTGRES_KEY")
PATH_ERROR = os.getenv("PATH_ERROR")
# import custom transformer for API data

logging.basicConfig(filename=PATH_ERROR, level=logging.ERROR,
                    format='%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')


# [START instantiate_dag]
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Loading weatherAPI to PostgresSQL with Airflow'],
)
def create_database_engine(postgres_key):
    '''
    This function creates a database engine
    '''
    try:
        database_engine = create_engine(postgres_key, echo=True)
        logging.info("Database engine created successfully.")
        return database_engine
    except Exception as e:
        logging.info(f"Error creating database engine: {e}")
        return None


engine = create_database_engine(POSTGRES_KEY)

Base = declarative_base()

def ETLWeatherPostgres():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    # [END instantiate_dag]
    # EXTRACT: Query the data from the Weather API
    @task()
    def extract():

        # TODO: Change the API Key to your key!!

        payload = {'Key': '5a91e86eedc148059a390511211510', 'q': 'Berlin', 'aqi': 'no'}
        r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

        # Get the json
        r_string = r.json()

        # weather_data_dict = json.loads(r_string)
        print(r_string)
        return r_string

    # TRANSFORM: Transform the API response into something that is useful for the load
    @task()
    def transform(weather_json: json):

        """
        A simple Transform task which takes in the API data and only extracts the location, wind,
        the temperature and time.
        """
        weather_str = json.dumps(weather_json)
        transformed_str = transform_weatherAPI(weather_str)

        # turn string into dictionary
        ex_dict = json.loads(transformed_str)

        # return ex_dict
        return ex_dict

        # LOAD: Save the data into Postgres database

    @task()
    def load(weather_data: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        writes it to the postgres database.
        """

        try:
            connection = psycopg2.connect(user="airflow",
                                          password="airflow",
                                          host="postgres",
                                          port="5432",
                                          database="WeatherData")
            cursor = connection.cursor()

            postgres_insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %s , %s, %s, %s);"""
            record_to_insert = (weather_data[0]["location"], weather_data[0]["temp_c"], weather_data[0]["wind_kph"],
                                weather_data[0]["timestamp"])
            cursor.execute(postgres_insert_query, record_to_insert)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error:

            print("Failed to insert record into mobile table", error)

            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

            raise Exception(error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    # Define the main flow
    weather_data = extract()
    weather_summary = transform(weather_data)
    load(weather_summary)


# Invocate the DAG
lde_weather_dag_posgres = ETLWeatherPostgres()
