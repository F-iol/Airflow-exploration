from airflow.sdk import dag,task
from datetime import datetime

@dag(
    dag_id  ='branches'
)
def branch_dag():


    @task.python
    def data_exctraction_task():

        weekend = datetime.now().weekday()
        is_weekend = weekend in [5,6]
        print(f'Today is {weekend+1} day of week')
        print('Exctracting data')
        extracted_data_dic = {'api_extracted_data':[1,2,3],
                              'db_extracted_data':[4,5,6],
                              's3_extracted_data':[7,8,9],
                              'weekend_flag':is_weekend}

        return extracted_data_dic
    
    @task.python
    def transform_api_task(loaded_data):
        data = loaded_data.get('api_extracted_data',[])
        print(f'Transforming data : {data}')
        data = [x*2 for x in data]
        data = {'api_extracted_data':data}
        return data
    
    @task.python
    def transform_db_task(loaded_data):
        data = loaded_data.get('db_extracted_data',[])
        print(f'Transforming data : {data}')
        data = [x*3 for x in data]
        data = {'db_extracted_data':data}
        return data
    
    @task.python
    def transform_s3_task(loaded_data):
        data = loaded_data.get('s3_extracted_data',[])
        print(f'Transforming data: {data}')
        data = [x-1 if x%2==0 else x+1 for x in data]
        data = {'s3_extracted_data':data}
        return data
    
    @task.python
    def merge_data(api_data,s3_data,db_data):
        return {
            **api_data,
            **s3_data,
            **db_data
        }


    @task.bash
    def load_task(final_data):
        print('Data transformed successfully and now being loaded to destination')
        api_data = final_data.get('api_extracted_data',[])
        db_data = final_data.get('db_extracted_data',[])
        s3_data = final_data.get('s3_extracted_data',[])

        return f" echo 'Loaded data: {api_data}, {db_data}, {s3_data}' "
    
    @task.bash
    def no_load_task():
        return "echo 'No Load task executed - it is weekend'"

    @task.branch
    def decider_task(data):
        flag = data.get('weekend_flag',[])
        if flag == True:
            return 'no_load_task'
        else:
            return 'load_task'
    

    extract = data_exctraction_task()
    transform_api = transform_api_task(extract)
    transform_db = transform_db_task(extract)
    transform_s3 = transform_s3_task(extract)
    merge = merge_data(transform_api,transform_db,transform_s3)
    status = load_task(merge)
    status_no_load = no_load_task()
    decision = decider_task(data =extract)
    merge >> decision >> [status,status_no_load]

branch_dag()
