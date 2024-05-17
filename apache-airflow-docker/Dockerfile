FROM apache/airflow:2.9.1

## Create virtual environment and install yfinance==0.2.38 inside it.
RUN python -m venv yfinance_venv && source yfinance_venv/bin/activate && \ 
pip install --no-cache-dir yfinance==0.2.38 && deactivate 

## Install all packages come under requirements.txt file
COPY requirements.txt .
RUN pip install --no-cache -r requirements.txt
