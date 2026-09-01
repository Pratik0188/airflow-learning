from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="bash_dag")
def bash_dag():

    @task.bash
    def first_task():
        return "echo 'Hello World!'"

    second_task = BashOperator(
        task_id="second_task",
        bash_command="echo 'This is the second task!'"
    )

    # Define the task dependency
    first_task_instance = first_task()
    first_task_instance >> second_task

# To run the DAG, we need to create an instance of it
bash_dag()