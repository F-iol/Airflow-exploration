from airflow.sdk import dag,task 

@dag(
    dag_id = 'first_dag',
)
def first_dag():
    
    @task.python
    def first_task():
        print('First task has been completed')

    @task.python
    def second_task():
        print('Second task has been completed')

    @task.python
    def third_task():
        print('Third one also has been completed')

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third 


first_dag()