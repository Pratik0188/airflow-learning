from airflow.sdk import dag, task, CronDataIntervalTimetable
import pendulum

@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(year=2026, month=4, day=15, tz="America/Halifax"),
    catchup=True
)
def incremental_load():

    @task.python
    def extract_data(**kwargs):
        from_date = kwargs['data_interval_start']
        to_date = kwargs['data_interval_end']
        print(f"Extracting data from {from_date} to {to_date}")
        print(f"SELECT * FROM source_table WHERE date >= '{from_date}' AND date < '{to_date}'")

    @task.bash
    def load_data():
        return """
        echo "Data loaded from {{ data_interval_start }} to {{ data_interval_end }}"
        """

    extract_data_instance = extract_data()
    load_data_instance = load_data()

    extract_data_instance >> load_data_instance

incremental_load_instance = incremental_load()