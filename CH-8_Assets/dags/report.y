import json

from airflow.sdk import dag, task
from etl import weather_asset


@dag(
    is_paused_upon_creation=False,
    schedule=[weather_asset],
)
def report():

    @task
    def read_data():
        with open("/opt/airflow/data/data.json", "r") as f:
            data = json.load(f)
        return data

    read_data()


report_dag = report()