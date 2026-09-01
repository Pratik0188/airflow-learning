from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag
def parent_dag():

    trigger_child_dag_first = TriggerDagRunOperator(
        task_id="trigger_child_dag_first",
        # This should match the dag_id of the child DAG (the function name, not the variable name)
        trigger_dag_id="child_dag_first",
    )

    trigger_child_dag_second = TriggerDagRunOperator(
        task_id="trigger_child_dag_second",
        trigger_dag_id="child_dag_second",
    )

    # Ensure child_dag_first completes before triggering child_dag_second
    trigger_child_dag_first >> trigger_child_dag_second


parent_dag_instance = parent_dag()