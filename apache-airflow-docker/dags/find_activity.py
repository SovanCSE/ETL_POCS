from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.models import Variable


API = "https://www.boredapi.com/api/activity"

@dag(
    start_date=datetime(2024,5,9),
    schedule='@daily',
    tags=['activity'],
    catchup=False
)
def find_activity():

    @task
    def get_activity():
        r = requests.get(API, timeout=10)
        return r.json()
    
    @task
    def write_activity(response):
        filepath = Variable.get('activity_file', default_var='/tmp/activity.txt')
        with open(filepath, 'a') as f:
            f.write(f"Today you will:: {response['activity']}\r\n")
        return filepath
    
    @task
    def read_activity(filepath):
        with open(filepath, 'r') as f:
            print(f'Read activity:: {f.read()}')

    activity_response = get_activity()
    filepath = write_activity(activity_response)
    read_activity(filepath)


find_activity()
