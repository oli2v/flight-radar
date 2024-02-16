FROM apache/airflow:2.8.1-python3.11
USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:root flight_radar /opt/airflow/dags/flight_radar
COPY run.py /opt/airflow/dags
COPY airflow_docker/dags/dags.py /opt/airflow/dags