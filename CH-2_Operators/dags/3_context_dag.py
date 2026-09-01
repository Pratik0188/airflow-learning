from airflow.sdk import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator


def first_task_func():
    return "Hello World!"


def second_task_func():
    return "Hello World!"


# --- Classic style: using "with DAG(...) as dag:" context manager ---
with DAG(
    dag_id="python_context_dag"
) as dag:

    first_task_op = PythonOperator(
        task_id="first_task",
        python_callable=first_task_func
    )

    second_task_op = PythonOperator(
        task_id="second_task",
        python_callable=second_task_func
    )

    first_task_op >> second_task_op


# --- Modern style: using the @dag decorator (TaskFlow API) ---
@dag(dag_id="python_dag")
def python_dag():

    first_task_op = PythonOperator(
        task_id="first_task",
        python_callable=first_task_func
    )

    second_task_op = PythonOperator(
        task_id="second_task",
        python_callable=second_task_func
    )

    first_task_op >> second_task_op


python_dag_instance = python_dag()