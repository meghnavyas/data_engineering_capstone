FROM apache/airflow:3.0.2

User root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk

RUN curl https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz -o spark-4.0.0-bin-hadoop3.tgz

# Change permissions of the downloaded tarball
RUN chmod 755 spark-4.0.0-bin-hadoop3.tgz

# Create the target directory and extract the tarball to it
RUN mkdir -p /opt/spark && tar xvzf spark-4.0.0-bin-hadoop3.tgz --directory /opt/spark --strip-components=1

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
