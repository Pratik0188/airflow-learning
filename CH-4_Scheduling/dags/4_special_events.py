from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum
from airflow.timetables.events import EventsTimetable

events_list_obj = EventsTimetable(event_dates=[
    pendulum.datetime(2026, 4, 15, tz="America/Halifax"),
    pendulum.datetime(2026, 4, 25, tz="America/Halifax"),
    pendulum.datetime(2026, 5, 5, tz="America/Halifax"),
])

@dag(
    schedule=events_list_obj,
    start_date=pendulum.datetime(year=2026, month=4, day=15, tz="America/Halifax"),
    catchup=False,
)
def schedule_special_events():

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

xcoms_auto_instance = schedule_special_events()