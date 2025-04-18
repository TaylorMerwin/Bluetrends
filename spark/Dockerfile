FROM bitnami/spark:3.5

# Install pip and required build dependencies if needed
USER root
RUN apt-get update && apt-get install -y python3-pip curl

# Copy exported requirements.txt from your project (make sure you've generated this file)
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt


# download the MySQL JDBC driver into Spark's jars folder
RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -L \
      https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
      -o ${SPARK_HOME}/jars/mysql-connector-java-8.0.28.jar

# copy your spark-defaults.conf if you still want to include spark.jars.packages:
COPY spark/spark-defaults.conf ${SPARK_HOME}/conf/