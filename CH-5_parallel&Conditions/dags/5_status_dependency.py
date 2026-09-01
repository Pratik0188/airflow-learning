from airflow.sdk import dag, task


@dag
def status_dependency_dag():

    @task.python
    def task_a():
        print("Executing task A")
        return "Task A completed"

    @task.python
    def task_b():
        print("Executing task B")
        raise ValueError("Simulating a failure in task B")

    @task.python(trigger_rule="all_done")
    def task_c(ti=None):
        a_result = ti.xcom_pull(task_ids="task_a")
        print("Task C running regardless of upstream status")
        print("Task A result was:", a_result)
        return "Notification sent"

    a = task_a()
    b = task_b()
    c = task_c()

    [a, b] >> c


status_dependency_dag_instance = status_dependency_dag()