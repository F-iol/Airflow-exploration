from airflow.sdk import dag,task

@dag(
    dag_id = 'kwargs_xcom_dag'
)
def kwargs_xcom_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs.get('ti')
        data= {'data':[1,2,3,4,5]}
        ti.xcom_push(key='return_value',value=data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs.get('ti')
        xcom_data = ti.xcom_pull(task_ids='first_task',key='return_value')
        if xcom_data:
            data=  xcom_data['data']
            print('data pulled successfully')

    t1 = first_task()
    t2 = second_task()

    t1>>t2

kwargs_xcom_dag()