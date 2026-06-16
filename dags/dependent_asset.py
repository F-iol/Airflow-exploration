from airflow.sdk import dag,task,asset
import pendulum
import os
from dag_asset import fetch_data


@asset(
    schedule=fetch_data,
    uri='/opt/airflow/logs/data/data_proccessed.txt',
    name='process_data'
)
def process_data(self):

    os.makedirs(os.path.dirname(self.uri),exist_ok=True)

    with open(self.uri,'w') as f:
        f.write(f"Data processed successfully at {pendulum.now('Europe/Warsaw')}")
    
    print(f'Data written to {self.uri}')