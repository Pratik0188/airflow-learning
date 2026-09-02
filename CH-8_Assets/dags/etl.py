import os

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, is_paused_upon_creation=False)
def etl():

    @task
    def extract():
        return {"data": {"city": "New York", "temperature": 30}}

    @task
    def transform(raw_data: dict):
        city = raw_data["data"]["city"]
        temp_celsius = raw_data["data"]["temperature"]
        temp_fahrenheit = (temp_celsius * 9 / 5) + 32
        return {"city": city, "temp_fahrenheit": temp_fahrenheit}

    @task
    def load(transformed_data: dict):
        print(f"loading data: {transformed_data}")

        # Creating directory to save the output
        output_dir = "/opt/airflow/data"
        os.makedirs(output_dir, exist_ok=True)

        output_file = f"{output_dir}/transformed_data.txt"
        with open(output_file, "w") as f:
            f.write(str(transformed_data))

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


etl()