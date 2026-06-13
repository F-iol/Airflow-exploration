from airflow.sdk import dag,task

@dag(
    dag_id = 'XCOM_dag'
)
def xcom_dag():

    @task.python
    def first_task():
        print("extracting data")
        data = {"data":[1,2,3,4,5]}
        return data
    
    @task.python
    def second_task(data):
        print('transforming data')
        data = data['data']
        transformed_data = data*2
        trans_data_dict = {'data':transformed_data}
        return trans_data_dict
    

    @task.python
    def third_task(data):
        loaded_data = data
        return loaded_data
    
    first = first_task()
    second=second_task(first)
    third = third_task(second)

    #first >> second >> third not needed

xcom_dag()
