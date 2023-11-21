FROM apache/airflow:2.7.3
USER root

RUN python3 --version

# Install OpenJDK 11 (for PySpark)
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python3 python3-dev python3-pip python3-virtualenv && \
    apt-get clean;

# Install Python dependencies (including PySpark)
USER airflow
COPY requirements.txt /requirements.txt
RUN \
    pip install --user --upgrade pip && \
    pip install -r /requirements.txt