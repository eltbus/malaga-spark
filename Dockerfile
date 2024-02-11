FROM ubuntu:latest

ARG SPARK_VERSION=spark-2.4.8
ARG HADOOP_VERSION=hadoop2.7
ARG JAVA_VERSION=openjdk-8-jre

WORKDIR /app

RUN apt update && apt install -y bash wget $JAVA_VERSION && \
    mkdir -p /opt/spark && \
    wget -q https://archive.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-$HADOOP_VERSION.tgz && \
    tar zxvf $SPARK_VERSION-bin-$HADOOP_VERSION.tgz -C /opt/spark --strip-components=1 && \
    rm $SPARK_VERSION-bin-$HADOOP_VERSION.tgz

ENV SPARK_HOME /opt/spark/
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

CMD ["/opt/spark/bin/spark-submit"]
