FROM apache/airflow:3.0.4-python3.11
COPY requirements.txt .
USER airflow
RUN python -m pip install --no-cache-dir -r requirements.txt 
USER root