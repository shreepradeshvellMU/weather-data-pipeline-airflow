FROM apache/airflow:2.9.3-python3.11
RUN pip install --no-cache-dir pandas pyarrow pymysql requests
