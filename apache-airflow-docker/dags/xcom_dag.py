# dags/xcom_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    print(type(ti))
    accuracy = uniform(0.1, 10.0)
    print(f'Model {ti.task_id} accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy',  value=accuracy)
    # return accuracy

def _choose_best_model(ti):
    print('choose best model')
    model_accuracies = ti.xcom_pull(key='model_accuracy', task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    print(f'model_accuracies:: {model_accuracies}')
    max_accuracy = max(model_accuracies)
    print(f'max accuracy:: {max_accuracy}')


    
with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    training_model_task = [
        PythonOperator(
            task_id=f'training_model_{task}',
            python_callable=_training_model
        ) for task in ['A', 'B', 'C']]

    choose_model = PythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

    downloading_data >> training_model_task >> choose_model
