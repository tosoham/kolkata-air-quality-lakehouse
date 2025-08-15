FROM apache/airflow:3.0.3-python3.11

USER root

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64


USER airflow

COPY ./requirements.txt /
RUN python -m pip install --upgrade pip && \ 
    pip install --no-cache-dir -r /requirements.txt

# COPY ./dags /opt/airflow/dags