FROM apache/airflow:2.10.5

USER root
# Install AWS CLI v2
RUN apt-get update && apt-get install -y awscli
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

