# from airflow.utils.dates import days_ago
# from airflow import DAG
# from datetime import timedelta
# from airflow.sensors.filesystem import FileSensor
# # from airflow.contrib.sensors.file_sensor import FileSensor
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.hooks.filesystem import FSHook
# from airflow.models import Variable
# import os
# from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor


# DAG_ID = 'GDRIVE_SENSOR_POC'
# ENV_ID = 'DEV'

# file_name = Variable.get("read_file_name", default_var='sample_data.csv')
# DRIVE_FILE_NAME = f"empty_{DAG_ID}_{ENV_ID}.txt".replace("-", "_")

# default_args = {
#     "depends_on_past" : False,
#     "start_date"      : days_ago( 1 ),
#     "retries"         : 1,
#     "retry_delay"     : timedelta( hours= 5 ),
# }



# def read_file_method(*args, **kwargs):
#     fs_con =  FSHook(fs_conn_id="my_file_system")
#     base_path = fs_con.get_path()
#     read_path = os.path.join(base_path, file_name)
#     print(f'read_path:: {read_path}')

# with DAG(
#     DAG_ID,
#     default_args=default_args,
#     tags=['ETL_POC'],
#     schedule_interval="@daily", 
#     catchup=False
#     ) as dag:

#    # [START detect_file]
#     detect_file = GoogleDriveFileExistenceSensor(
#         task_id="detect_file",
#         gcp_conn_id=CONNECTION_ID,
#         folder_id="",
#         file_name=DRIVE_FILE_NAME,
#     )
#     # [END detect_file]

#     t2 = PythonOperator(
#         task_id="read_file",
#         python_callable=read_file_method
#         # provide_context=True
#         # op_kwargs={'a':10},
#         # op_args=[20]
#     )

#     t1 >> t2
