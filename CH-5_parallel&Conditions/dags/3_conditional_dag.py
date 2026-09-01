from airflow.sdk import dag, task


@dag
def parallel_dag():

    @task.bash
    def task_bash():
        return "echo 'Hello from Bash'"

    @task.python
    def fetch_api():
        return {"type": "api", "data": ["data1", "data2", "data3"]}

    @task.python
    def fetch_db():
        return {"type": "db", "data": ["data4", "data5", "data6"]}

    @task.python
    def fetch_s3():
        return {"type": "s3", "data": ["data7", "data8", "data9"]}

    @task.python
    def process_data(api_data, db_data, s3_data):
        print("Processing API Data:", api_data)
        print("Processing DB Data:", db_data)
        print("Processing S3 Data:", s3_data)

        combined = api_data["data"] + db_data["data"] + s3_data["data"]
        return combined  # returned so XCom has something to pull downstream

    @task.branch
    def load_data_branch(ti=None):
        processed_data = ti.xcom_pull(task_ids="process_data")
        if processed_data and len(processed_data) > 10:
            return "s3_load"
        return "glue_load"

    @task.python
    def s3_load():
        print("Loading to S3...")

    @task.python
    def glue_load():
        print("Loading to Glue...")

    # --- instantiate each task exactly once ---
    bash_result = task_bash()

    api_data = fetch_api()
    db_data = fetch_db()
    s3_data = fetch_s3()

    processed = process_data(api_data, db_data, s3_data)
    branch = load_data_branch()

    load_s3 = s3_load()
    load_glue = glue_load()

    # --- dependencies ---
    bash_result >> [api_data, db_data, s3_data]
    processed >> branch >> [load_s3, load_glue]


parallel_dag_instance = parallel_dag()