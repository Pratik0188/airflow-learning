import os
from datetime import datetime

from airflow.sdk import dag, task
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(schedule=None, catchup=False)
def etl_pipeline():
    @task
    def timestamp():
        return datetime.now().isoformat()

    @task
    def extract(ts: str):
        url = "http://fastapi:8000/fetch_data"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", [])

        os.makedirs("/tmp/raw", exist_ok=True)
        safe_ts = ts.replace(":", "-")  # avoid ':' in filenames
        file_path = f"/tmp/raw/data_{safe_ts}.csv"

        df = pd.DataFrame(data, columns=["id", "name", "age"])
        df.to_csv(file_path, index=False)

        return file_path

    @task
    def transform(file_path: str):
        df = pd.read_csv(file_path)

        df["age_group"] = df["age"].apply(lambda x: "Young" if x < 30 else "Adult")

        os.makedirs("/tmp/processed", exist_ok=True)
        out_name = os.path.basename(file_path).replace("data_", "processed_")
        out_path = f"/tmp/processed/{out_name}"
        df.to_csv(out_path, index=False)
        return out_path

    @task
    def create_tables():
        query = """
        CREATE TABLE IF NOT EXISTS employees (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            age_group VARCHAR(50)
        );
        """

        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        # engine.begin() opens a transaction and automatically commits on
        # success or rolls back on exception -- no need to manage it manually.
        with engine.begin() as conn:
            conn.execute(text(query))

    @task
    def load(file_path: str):
        df = pd.read_csv(file_path)

        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        df.to_sql("employees", con=engine, if_exists="append", index=False)

    # Task to create a table in PostgreSQL using the classic SQL operator
    # instead of raw sqlalchemy -- conn_id must match a Connection you've
    # registered in Airflow (Admin -> Connections), e.g. "mypostgresql".
    create_new_table = SQLExecuteQueryOperator(
        task_id="create_students_table",
        conn_id="mypostgresql",
        sql="""
            CREATE TABLE IF NOT EXISTS students (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                age_group VARCHAR(50)
            );
        """,
    )

    # Task to write the transformed data into the "students" table using
    # PostgresHook.copy_expert, which bulk-loads a CSV file directly via
    # Postgres's COPY command. Takes the transformed file path as an
    # argument (like `load` does) instead of pulling it via XCom, so the
    # dependency graph stays correct.
    @task
    def write_to_new_table(file_path: str):
        hook = PostgresHook(postgres_conn_id="mypostgresql")
        hook.copy_expert(
            sql="""
                COPY students(id, name, age, age_group)
                FROM STDIN WITH CSV HEADER
            """,
            filename=file_path,
        )

    ts = timestamp()
    raw_path = extract(ts)
    transformed_path = transform(raw_path)

    tables_ready = create_tables()
    tables_ready >> load(transformed_path)

    # create_new_table must run before we COPY into "students"
    create_new_table >> write_to_new_table(transformed_path)


etl_pipeline()