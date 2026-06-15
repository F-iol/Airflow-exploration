from airflow.sdk import dag,task
import pendulum
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
     dag_id='incremental_dag',
     schedule=CronDataIntervalTimetable('@daily',timezone='Europe/Warsaw'),
     start_date=pendulum.datetime(year=2026,month=6,day=1,tz='Europe/Warsaw'),
     end_date=pendulum.datetime(year=2026,month=6,day=18,tz='Europe/Warsaw'),
     catchup=True
)
def incremental_load_dag():


    @task.python
    def incremental_data_fetch(**kwargs):
        print(f'Fetching data from {kwargs['data_interval_start']} to {kwargs['data_interval_end']}')

    @task.bash
    def incremental_data_process():
        return """
                    echo "Processing incremental data from {{data_interval_start}} to {{data_interval_end}}"
                """
    


    t1 = incremental_data_fetch()
    t2 = incremental_data_process()


    t1>>t2

incremental_load_dag()