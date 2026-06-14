from airflow.sdk import dag,task
from pendulum import datetime

@dag(
    dag_id = 'scheduled_dag',
    start_date = datetime(year=2026,month=6,day=14,tz='Europe/Warsaw'),
    schedule='@daily'
)
def schedule_dag():

    @task.python
    def first_task():
        print('starting scheduled dag') 
    
    @task.python
    def second_task():
        print('processing data')
    
    @task.python
    def third_task():
        print('ending scheduled dag')

    
    f1 = first_task()
    f2=second_task()
    f3 = third_task()

    f1>>f2>>f3

schedule_dag()