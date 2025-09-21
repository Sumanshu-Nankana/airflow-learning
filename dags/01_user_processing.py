import csv
from datetime import datetime

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue


@dag
def user_processing():

    # Execute the SQL Query
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",  # This connection-id we need to create with Postgres from Airflow.
        sql="""
        CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        firstname VARCHAR(255),
        lastname VARCHAR(255),
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    )

    # Defining a Sensor
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(
            "https://raw.githubusercontent.com/Sumanshu-Nankana/airflow-learning/refs/heads/main/datasets/fakeuser.json"
        )
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None

        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER", filename="/tmp/user_info.csv"
        )

    # Create the Task Instance
    api_sensor_task = is_api_available()
    extract_task = extract_user(api_sensor_task)
    process_task = process_user(extract_task)
    store_task = store_user()

    # Define the task dependencies
    create_table >> api_sensor_task >> extract_task >> process_task >> store_task


# if we don't call the function, it will not show on the Airflow UI
# Call the main function to register the DAG
user_processing()
