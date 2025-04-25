FROM bitnami/spark:3.5

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt



RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -L \
      https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
      -o ${SPARK_HOME}/jars/mysql-connector-java-8.0.28.jar


COPY spark/spark-defaults.conf ${SPARK_HOME}/conf/