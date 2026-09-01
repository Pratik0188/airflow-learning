from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    schedule=DeltaTriggerTimetable(delta=pendulum.duration(days=10)),
    start_date=pendulum.datetime(year=2026, month=4, day=15, tz="America/Halifax"),
    catchup=False,
)
def schedule_catchup():

    @task.python
    def fetch_data():
        # Simulate fetching data from an API
        data = {"name": "Airflow", "version": "3.0"}
        # Just return it — Airflow automatically pushes this to XCom
        return data

    @task.python
    def process_data(fetched_data):
        # Airflow automatically pulls the upstream task's return value
        # and passes it in directly as an argument
        processed_data = f"Processed {fetched_data['name']} version {fetched_data['version']}"
        print(processed_data)

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a bash task!'"
    )

    # Define task dependencies
    fetch_data_instance = fetch_data()
    process_data_instance = process_data(fetch_data_instance)

    process_data_instance >> bash_task

xcoms_auto_instance = schedule_catchup()