from airflow.sdk import dag,task,asset,Asset
import pendulum
import os
#from dags.dag_asset import fetch_data

upstream_asset = Asset(
        name='fetch_data',
        uri='/opt/airflow/logs/data/data_extract.txt'
    )

@asset(
    schedule=upstream_asset,
    uri='/opt/airflow/logs/data/data_proccessed.txt',
    name='process_data'
)
def process_data(self):

    os.makedirs(os.path.dirname(self.uri),exist_ok=True)

    with open(self.uri,'w') as f:
        f.write(f"Data processed successfully at {pendulum.now('Europe/Warsaw')}")
    
    print(f'Data written to {self.uri}')