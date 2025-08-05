# Use the official Apache Airflow image as a base
FROM apache/airflow:2.8.1

# Switch to root user to install dependencies
USER root

# Install OpenJDK 17, which is the standard version for the Debian 12 (Bookworm) base image.
# Set DEBIAN_FRONTEND to noninteractive to prevent prompts during installation.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable to the correct path for OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user
USER airflow

# Copy requirements file and install Python packages
COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt