FROM python:3.8-bullseye as spark-base

ARG SPARK_VERSION=3.4.2

# Install tools required by the OS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Setup the directories for Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz


FROM spark-base as pyspark

# Install Python dependencies
COPY ./spark-requirements.txt ${SPARK_HOME}/requirements.txt
#RUN pip3 install -r requirements.txt

ENV PATH=${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}
ENV SPARK_HOME=${SPARK_HOME}
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY spark-defaults.conf "${SPARK_HOME}/conf"

RUN mkdir ${SPARK_HOME}/spark-events
RUN chmod 777 "${SPARK_HOME}/spark-events"

RUN chmod u+x ${SPARK_HOME}/sbin/* && \
    chmod u+x ${SPARK_HOME}/bin/*

ENV PYTHONPATH=${SPARK_HOME}/python/:$PYTHONPATH

COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
