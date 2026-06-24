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

# pyspark is a 317 MB sdist — needs a long timeout; separate layer so it caches independently.
RUN pip install --no-cache-dir --timeout 600 --retries 10 pyspark==3.5.3

COPY ./requirements.txt /
RUN pip install --no-cache-dir --timeout 180 --retries 5 -r /requirements.txt
