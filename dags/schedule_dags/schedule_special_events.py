from airflow.sdk import dag,task
import pendulum
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates=
    [
    pendulum.datetime(2026,1,1),
    pendulum.datetime(2026,5,5),
    pendulum.datetime(2026,8,21),
    pendulum.datetime(2026,12,14)
    ]
    )




@dag(
    dag_id = 'special_event_dag',
    schedule=special_dates,
    start_date = pendulum.datetime(year=2026,month=1,day=1,tz='Europe/Warsaw'),
    end_date = pendulum.datetime(year=2026,month=12,day=31,tz='Europe/Warsaw'),
    catchup=True,
    is_paused_upon_creation=False
)
def info_dag():
    
    @task.python
    def display_info(**kwargs):
        today_date = kwargs['logical_date']
        print(f'Today - {today_date} is the special day!')

    t1 = display_info()

info_dag()