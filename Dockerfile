FROM apache/airflow:2.9.3-python3.12 

WORKDIR /opt/airflow

RUN mkdir -p solutions


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY solutions/task2_build.py solutions/
COPY solutions/task2_validate.py solutions/
COPY solutions/task2_ingest.sh solutions/

COPY db_utils.py /opt/airflow/db_utils.py
