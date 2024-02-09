FROM bitnami/spark:3.5.0

USER root

COPY ./flight_radar /opt/bitnami/spark/flight_radar
COPY ./run.py /opt/bitnami/spark/

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

RUN chmod -R 777 /opt/bitnami/spark/flight_radar
