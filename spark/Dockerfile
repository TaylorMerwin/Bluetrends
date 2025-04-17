FROM bitnami/spark:3.5

# Install pip and required build dependencies if needed
USER root
RUN apt-get update && apt-get install -y python3-pip curl

# Copy exported requirements.txt from your project (make sure you've generated this file)
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/spark-defaults.conf ${SPARK_HOME}/conf/