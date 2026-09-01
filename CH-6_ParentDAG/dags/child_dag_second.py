import os

from airflow.sdk import dag, task


@dag(is_paused_upon_creation=False)
def child_dag_second():

    @task.python
    def task_pre():
        print("Task A")

    @task.python
    def task_write():
        print("Task B")

        # Creating directory if it doesn't exist
        os.makedirs("/tmp/data", exist_ok=True)

        # Writing to a file
        with open("/tmp/data/data_second.txt", "w") as f:
            f.write("This is the second child DAG")

    # Setting task dependencies
    task_pre() >> task_write()


child_dag_second_dag = child_dag_second()