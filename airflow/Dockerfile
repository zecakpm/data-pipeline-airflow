FROM apache/airflow:2.9.1-python3.12

USER airflow
COPY requirements.txt /tmp/requirements.txt 
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

