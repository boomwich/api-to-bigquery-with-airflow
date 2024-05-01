FROM apache/airflow:2.9.0
COPY requirement.txt .
RUN pip install --no-cache-dir -r ./requirement.txt