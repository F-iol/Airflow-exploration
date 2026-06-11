from airflow.sdk import dag,task


@dag(
    dag_id='version_dag'
)
def version_dag():

    @task.python
    def first_task():
        print('first task completed')
    
    @task.python
    def second_task():
        print('also completed')
    
    @task.python
    def third_task():
        print('DAG completed')


    @task.python
    def version_task():
        print('Version changed')

    @task.python
    def test_task():
        print('actually it didnt change ') # here it changed

    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()
    test = test_task()


    first >> second >> third >>version >>test

version_dag()