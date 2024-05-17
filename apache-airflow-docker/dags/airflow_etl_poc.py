from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

## Python in-build packages
from airflow.models import Variable
from datetime import timedelta
import os, shutil


default_args = {
    "depends_on_past" : False,
    "start_date"      : days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : timedelta( hours= 5 )
}

file_name = Variable.get("read_file_name", default_var=None)


def snowflake_connector():
    sf_hook = SnowflakeHook(snowflake_conn_id='my_snowflake_connection')
    conn = sf_hook.get_conn()
    return conn


def query_to_snowflake(query):
    try:
        conn = snowflake_connector()
        cur = conn.cursor()
        print(f'Query:: {query}')
        result = cur.execute(query)
        return result.fetchall()
    except Exception as e:
        raise ValueError(f'ERROR while invoking query to snowflake:: {e}')
    finally:
        # Close connections
        cur.close()
        conn.close()    


def load_datafiles_to_stage(ti, *args, **kwargs):
    fs_con = FSHook(fs_conn_id="my_file_system")
    base_path = fs_con.get_path()
    ti.xcom_push(key='read_base_path', value=base_path)
    read_base_path = ti.xcom_pull(key='read_base_path', task_ids='read_file_path')
    print(f'read_base_path:: {read_base_path}')
   
    ## CREATE NAMED STAGE IF NOT EXISTS
    named_stage_create_query = """ 
    CREATE STAGE IF NOT EXISTS SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_NAMED_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV', 
        FIELD_DELIMITER = ',', 
        SKIP_HEADER = 1
    )
    ;
    """
    query_to_snowflake(named_stage_create_query)
    print(f"Named stage 'TRAVEL_HISTORY_NAMED_STAGE' created successfully ")

    ## Loading local data files to snowflake named stage
    put_command = f"PUT file://{read_base_path}/travel_history*.csv @TRAVEL_HISTORY_NAMED_STAGE AUTO_COMPRESS=TRUE"
    query_to_snowflake(put_command)
    print('data file loaded to internal named staged')


def remove_local_file(ti):
    read_base_path = ti.xcom_pull(key='read_base_path', task_ids='read_file_path')
    prefix = file_name.split('*')[0]
    print(f'read_base_path:: {read_base_path} and prefix:: {prefix}')

    # Get a list of all files in the directory
    files = os.listdir(read_base_path)
    print(f'files:: {files}')

    # Filter out files with the specified prefix
    files_to_remove = [file for file in files if file.lower().startswith(prefix.lower())]
    
    # Remove each file
    for file_to_remove in files_to_remove:

        source_file = os.path.join(read_base_path, file_to_remove)
        destination_dir = os.path.join(read_base_path, 'processed_datafiles')

        # Ensure the destination directory exists
        os.makedirs(destination_dir, exist_ok=True)

        # move the file
        shutil.move(source_file, destination_dir)
        print(f"File moved from {source_file} to {destination_dir}")

        # os.remove(source_file)
        # print(f"Removed file: {source_file}")


def load_staged_datafiles_to_rawtable():
    
    create_table_query = """ 
        CREATE TABLE IF NOT EXISTS SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY (
        PersonID Varchar(10),
        PersonName	Varchar(255),
        FlightID	Varchar(10),
        Source	Varchar(10),
        Destination	Varchar(10),
        TravelDate DATE,
        SourceFilename Varchar(255),
        InsertTS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP
    );
    """
    query_to_snowflake(create_table_query)
    print(" 'SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY' table created successfully!")
    

    create_stream_from_base_history = """
     CREATE OR REPLACE STREAM SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_STREAM 
    ON TABLE SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY APPEND_ONLY = TRUE
    ;
    """
    query_to_snowflake(create_stream_from_base_history)
    print(" 'SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_STREAM' stream created successfully!")
    

    load_to_raw_tbl_query = """
    COPY INTO SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY (PersonID, PersonName, FlightID, Source, Destination, TravelDate, SourceFilename)
    FROM (
    SELECT $1, $2, $3, $4, $5, $6, metadata$filename FROM @TRAVEL_HISTORY_NAMED_STAGE
    ) 
    PATTERN='.*travel_history.*[.]csv.*'
    ;
    """
    query_to_snowflake(load_to_raw_tbl_query)
    print("Staged data files loaded to snowflake raw table 'SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY' ")


def load_historical_data_table():
    
    invoke_stored_procedure = 'CALL SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_SP();'
    query_to_snowflake(invoke_stored_procedure)
    print(" 'SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD2_SP()' executed successfully!")


def load_latest_data_table():
    invoke_stored_procedure = 'CALL SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1_SP();'
    query_to_snowflake(invoke_stored_procedure)
    print(" 'SAMPLE_DB.SAMPLE_SCHEMA.TRAVEL_HISTORY_SCD1_SP()' executed successfully!")


with DAG(
    "airflow_elt_sample_workflow",
    default_args = default_args,
    tags=["ETL_POC"],
    schedule='@daily', 
    catchup=False
    ) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file", 
        fs_conn_id="my_file_system",
        filepath=file_name, ## File file having extension of csv
        poke_interval=30, ## Poke in every 30 sec
        timeout=60*30 ## wait upto 30 mins 
    )

    read_file_path = PythonOperator(
        task_id="read_file_path",
        python_callable=read_file_method
    )
    
    load_to_sf_stage = PythonOperator(
        task_id="load_to_sf_stage",
        python_callable=load_datafiles_to_stage
    )

    load_into_sf_rawtable =  PythonOperator(
        task_id="load_into_sf_rawtable",
        python_callable=load_staged_datafiles_to_rawtable
    )
     
    clean_data_file_from_local = PythonOperator(
        task_id="clean_data_file_from_local",
        python_callable=remove_local_file
    )

    load_scd2_table = PythonOperator(
        task_id="load_historical_data_table",
        python_callable=load_historical_data_table
    )

    load_scd1_table = PythonOperator(
        task_id="load_latest_data_table",
        python_callable=load_latest_data_table
    )
    
    wait_for_file >> read_file_path >> load_to_sf_stage >> load_into_sf_rawtable >> clean_data_file_from_local >> load_scd2_table >> load_scd1_table    
