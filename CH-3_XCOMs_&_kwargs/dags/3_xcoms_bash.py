from airflow.sdk import dag, task, get_current_context
from airflow.operators.bash import BashOperator

@dag
def xcoms_bash():

    @task.python
    def fetch_data():
        # Simulate fetching data from an API
        data = {"name": "Airflow", "version": "3.0"}

        # Pushing data to XCOM manually
        context = get_current_context()
        ti = context["ti"]
        ti.xcom_push(key="fetched_data", value=data)

        return data

    @task.python
    def process_data(ti=None):
        # Pull the data from XCOM manually
        pulled_data = ti.xcom_pull(key='fetched_data', task_ids="fetch_data")

        # Simulate processing the data
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)

    # Pull XCom data into a BashOperator using Jinja templating
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command=(
            "echo 'Fetched data name: "
            "{{ ti.xcom_pull(key=\"fetched_data\", task_ids=\"fetch_data\")[\"name\"] }}' "
            "&& echo 'Fetched data version: "
            "{{ ti.xcom_pull(key=\"fetched_data\", task_ids=\"fetch_data\")[\"version\"] }}'"
        )
    )

    # Define task dependencies
    fetch_data_instance = fetch_data()
    process_data_instance = process_data()

    fetch_data_instance >> process_data_instance >> bash_task

xcoms_manual_instance = xcoms_bash()