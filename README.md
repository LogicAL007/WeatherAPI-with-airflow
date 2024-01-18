# Airflow ETL Pipeline: WeatherAPI to PostgreSQL  with Taskflow

## Overview
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to extract weather data from an external API (WeatherAPI), transform it, and then load it into a PostgreSQL database. It also includes a task for printing the transformed data to logs. This pipeline is useful for regularly updating a database with the latest weather information.

## Requirements
- Python 3.x
- Apache Airflow
- PostgreSQL
- Python Libraries: `pendulum`, `requests`, `psycopg2`, `json`, `os`
- A `.env` file for environment variables

## Setup
1. **Environment Variables**: Set up your `.env` file with the following variables:
   ```
   API_KEY=your_weatherapi_key
   USER_PASSWORD=your_postgres_password
   USER=your_postgres_username
   DB_NAME=your_postgres_dbname
   ```

2. **Python Dependencies**: Install the required Python libraries (listed in `requirements.txt`).

3. **Airflow Setup**: Configure Apache Airflow in your environment. Ensure Airflow is installed and properly configured to run DAGs.

4. **Database Setup**: Ensure that PostgreSQL is installed and running. Create a table named `temperature` in your PostgreSQL database with appropriate columns (`location`, `temp_c`, `wind_kph`, `time`).

## DAG Configuration
- **DAG ID**: `ETLWeatherPostgresAndPrint`
- **Start Date**: January 1, 2024
- **Schedule Interval**: None (manual triggering)
- **Tags**: `Loading weatherAPI to PostgresSQL with Airflow`

## DAG Tasks
1. **Extract**: Connects to WeatherAPI using the provided API key, queries for weather data for a specified location (Berlin in this case), and extracts the JSON response.

2. **Transform**: Processes the JSON response to extract relevant information (location, wind, temperature, and time) and converts it into a suitable format for database insertion.

3. **Load**: Connects to the PostgreSQL database and inserts the transformed data into the `temperature` table.

4. **Query and Print**: Logs the transformed weather data. This is useful for debugging and monitoring purposes.

## Execution
To run the DAG, ensure your Airflow environment is active and the DAG is correctly placed in your Airflow's DAG directory. Trigger the DAG manually through the Airflow UI. i attached a docker compose file you can spin up on docker to get your airflow and postgres running.

## Notes
- This pipeline is designed for educational and demonstration purposes and may require modifications for production use.
- Ensure that your API key and database credentials are kept secure and not exposed in your code.

