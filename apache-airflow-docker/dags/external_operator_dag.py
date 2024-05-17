from airflow.decorators import dag, task
from datetime import datetime 
import yfinance as yf


@dag(
    start_date=datetime(2024,5,13),
    schedule=None,
    catchup=False,
    tags =['example'],
    description='ExternalPythonOperator'
)
def simple_external_operator_dag():
  
    @task
    def extract_stock_old_finance():
        print(yf.download('APPL', start='2024-03-01', end='2024-03-30'))
        # appl = yf.Ticker('APPL')
        # appl.get_shares_full(start='2020-12-01', end='2020-12-31')
    
    @task.external_python('/opt/airflow/yfinance_venv/bin/python')
    def extract_stock_new_finance():
        import yfinance as yf
        print(yf.download('APPL', start='2024-03-01', end='2024-03-30'))
        appl = yf.Ticker('APPL')
        appl.get_shares_full(start='2020-12-01', end='2020-12-31')
   
    extract_stock_old_finance()
    extract_stock_new_finance()


simple_external_operator_dag()
