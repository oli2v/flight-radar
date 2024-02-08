FROM bitnami/spark:3.5.0

USER root

RUN apt update \
    && apt install -y openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/ 

COPY ./flight_radar /opt/bitnami/spark/flight_radar
COPY ./run.py /opt/bitnami/spark/

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

RUN chmod -R 777 /opt/bitnami/spark/flight_radar
