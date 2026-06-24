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

# Install pyspark from a pre-built local wheel (avoids 317 MB download inside the Docker VM).
COPY --chown=airflow:root ./wheels/pyspark-3.5.3-py2.py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/pyspark-3.5.3-py2.py3-none-any.whl && \
    rm /tmp/pyspark-3.5.3-py2.py3-none-any.whl

COPY ./requirements.txt /
RUN pip install --no-cache-dir --timeout 120 -r /requirements.txt
