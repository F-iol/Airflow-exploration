from airflow.sdk import dag,task 

@dag(
    dag_id = 'dag_orch_1',
)
def operators_dag_v1():

    @task.bash
    def open_git():

        return 'echo https://github.com/F-iol'

    @task.python
    def info():
        print('Git returned successfuly')

    first = open_git()
    second = info()

    first >> second

operators_dag_v1()
    


