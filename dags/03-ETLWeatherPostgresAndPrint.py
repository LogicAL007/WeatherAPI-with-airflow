
# imports important for Airflow
import pendulum
from airflow.decorators import dag, task

# Import Modules for code
import json
import requests
import psycopg2 
import os
# import custom transformer for API data
from transformer import transform_weatherAPI
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
USER_PASSWORD = os.getenv('USER_PASSWORD')
USERNAME = os.getenv('USER')
DB_NAME = os.getenv('DB_NAME')
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Loading weatherAPI to PostgresSQL with Airflow'],
)
def ETLWeatherPostgresAndPrint():
    # EXTRACT: Query the data from the Weather API
    @task()
    def extract():
        payload = {'Key': API_KEY, 'q': 'Berlin', 'aqi': 'no'}
        r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

        # Get the json
        r_string = r.json()

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
        
        #return ex_dict
        return ex_dict     

    # LOAD: Save the data into Postgres database
    @task()
    def load(weather_data: dict):
        """
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to the postgres database.
        """

        try:
            connection = psycopg2.connect(user=USERNAME,
                                        password=USER_PASSWORD,
                                        host="postgres",
                                        port="5432",
                                        database=DB_NAME)
            cursor = connection.cursor()

            postgres_insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %s , %s, %s, %s);"""
            record_to_insert = (weather_data[0]["location"], weather_data[0]["temp_c"], weather_data[0]["wind_kph"], weather_data[0]["timestamp"])
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

    @task()
    def query_print(weather_data: dict):
        """
        This just prints out the result into the log (even without instantiating a logger)
        """
        print(weather_data)


    
    # Define the main flow
    weather_data = extract()
    weather_summary = transform(weather_data)
    load(weather_summary)
    query_print(weather_summary)


# Invocate the DAG
lde_weather_dag_postgres = ETLWeatherPostgresAndPrint()

