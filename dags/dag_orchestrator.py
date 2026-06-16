from airflow.sdk import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id = 'dag_orchestrator',
)
def dag_orchestrator():
    
    trigger_frist_dag = TriggerDagRunOperator(
        task_id='trigger_first_orch_dag',
        trigger_dag_id = 'dag_orch_1'
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_second_orch_dag',
        trigger_dag_id = 'dag_orch_2'
    )
    
    trigger_frist_dag>>trigger_second_dag

dag_orchestrator()